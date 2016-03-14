package logging

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/pathutils"
)

// Send is the log directory for send log entries to match legacy STS.
const Send = "outgoing_to"

// Receive is the log directory for incoming log entries to match legacy STS.
const Receive = "incoming_from"

// Disk is the log directory for disk read/write messages to match legacy STS.
const Disk = "to_disk"

// Msg is the log directory for debug and error messages to match legacy STS.
const Msg = "messages"

var gLoggers map[string]*Logger

// Init will create a logger for each input "mode".
func Init(modes []string, basePath string, debug bool) {
	if gLoggers == nil {
		gLoggers = make(map[string]*Logger)
	}
	for _, mode := range modes {
		_, exists := gLoggers[mode]
		if !exists {
			gLoggers[mode] = newLogger(basePath, mode, debug)
		}
	}
}

// Done terminates logging for all created loggers.
func Done() {
	if gLoggers == nil {
		return
	}
	for _, logger := range gLoggers {
		logger.close()
	}
}

// GetLogger returns the logger for the specified mode.
func GetLogger(mode string) *Logger {
	return gLoggers[mode]
}

// Debug joins each item in params{} by a space, prepends a timestamp and the location of the message,
// and writes the result to stdout and to the "messages" log file.
func Debug(params ...interface{}) {
	logger, exists := gLoggers[Msg]
	if !exists || !logger.debug {
		return
	}
	logger.lock.Lock()
	defer logger.lock.Unlock()
	_, file, line, _ := runtime.Caller(1) // Get file name and line number of the caller of this function
	params = append([]interface{}{fmt.Sprintf("%s:%d", filepath.Base(file), line)}, params...)
	fmt.Fprintln(os.Stdout, params...)
	logger.rotate()
	logger.logger.Println(params...)
}

// Error joins each item in params{} by a space, prepends a timestamp and the location of the error,
// and writes the result to stderr and to the "messages" log file.
func Error(params ...interface{}) {
	logger, exists := gLoggers[Msg]
	if !exists {
		fmt.Fprintln(os.Stderr, params...) // Prevent error message from being lost.
		return
	}
	logger.lock.Lock()
	defer logger.lock.Unlock()
	_, file, line, _ := runtime.Caller(1) // Get file name and line number of the caller of this function
	params = append([]interface{}{fmt.Sprintf("%s:%d", filepath.Base(file), line)}, params...)
	fmt.Fprintln(os.Stderr, params...)
	logger.rotate()
	logger.logger.Println(params...)
}

// Disked writes a formatted string to the "to_disk" log file.
func Disked(msg string, hash string, size int64, prefix ...string) {
	logger, exists := gLoggers[Disk]
	if !exists {
		return
	}
	logger.lock.Lock()
	defer logger.lock.Unlock()
	logger.rotate(prefix...)
	line := fmt.Sprintf("%s:%s:%d:%d", msg, hash, size, time.Now().Unix())
	logger.logger.Printf(line)
	logger.lock.Lock()
}

// Sent writes a formatted string to the "outgoing_to" log file.
func Sent(msg string, hash string, size int64, ms int64, prefix ...string) {
	logger, exists := gLoggers[Send]
	if !exists {
		return
	}
	logger.lock.Lock()
	defer logger.lock.Unlock()
	logger.rotate(prefix...)
	line := fmt.Sprintf("%s:%s:%d:%d: %d ms", msg, hash, size, time.Now().Unix(), ms)
	logger.logger.Printf(line)
	logger.fh.Sync()
}

// Received writes a formatted string to the "incoming_from" log file.
func Received(msg string, hash string, size int64, prefix ...string) {
	logger, exists := gLoggers[Receive]
	if !exists {
		return
	}
	logger.lock.Lock()
	defer logger.lock.Unlock()
	logger.rotate(prefix...)
	line := fmt.Sprintf("%s:%s:%d:%d:", msg, hash, size, time.Now().Unix())
	logger.logger.Println(line)
	logger.fh.Sync()
}

func getPath(basePath string, mode string, time time.Time, prefix ...string) string {
	root := pathutils.Join(basePath, mode)
	if len(prefix) > 0 {
		root = pathutils.Join(root, pathutils.Join(prefix...))
	}
	return pathutils.Join(root, getYear(time)+getMonth(time), getDay(time))
}

// Logger is the main struct for managing a rolling log file.
type Logger struct {
	logger   *log.Logger
	fh       *os.File
	lock     sync.RWMutex
	path     string
	basePath string
	mode     string
	debug    bool
}

func newLogger(basePath string, mode string, debug bool) *Logger {
	logger := Logger{}
	logger.lock = sync.RWMutex{}
	logger.basePath = basePath
	logger.mode = mode
	logger.debug = debug
	if mode == Msg {
		if basePath != "" {
			logger.logger = log.New(nil, "", log.Ldate|log.Ltime)
		}
	} else {
		logger.logger = log.New(nil, "", 0)
	}
	return &logger
}

func (logger *Logger) getPath(time time.Time, prefix ...string) string {
	return getPath(logger.basePath, logger.mode, time, prefix...)
}

func (logger *Logger) getCurrPath(prefix ...string) string {
	now := time.Now()
	return logger.getPath(now, prefix...)
}

func (logger *Logger) rotate(prefix ...string) {
	currPath := logger.getCurrPath(prefix...)
	_, err := os.Stat(currPath)
	if logger.path != currPath || os.IsNotExist(err) || logger.fh == nil {
		logger.close()
		logger.path = currPath
		os.MkdirAll(filepath.Dir(currPath), os.ModePerm)
		logger.fh, err = os.OpenFile(currPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("Failed to open log file: %s", err.Error()))
		}
		logger.logger.SetOutput(logger.fh)
	}
}

func (logger *Logger) close() {
	if logger.fh != nil {
		logger.fh.Close()
	}
}

// Search will look for a given text pattern in the log history.
// If "start" is nonzero it will search from start to current.
// If "start" is zero it will search from current back nDays.
func (logger *Logger) Search(text string, nDays int, start int64, prefix ...string) bool {
	logger.lock.RLock()
	defer logger.lock.RUnlock()
	fwd := true
	now := time.Now().Unix()
	if start == 0 {
		start = now
		fwd = false
	} else {
		d0 := int(now/86400) + 1
		d1 := int(start / 86400)
		nDays = d0 - d1
	}
	if nDays == 0 {
		return false
	}
	logs := make([]string, nDays)
	start -= start % 86400
	for i := range logs {
		offset := int64(3600 * 24 * i)
		if !fwd {
			offset *= -1
		}
		timestamp := time.Unix(start+offset, 0)
		logs[i] = logger.getPath(timestamp, prefix...)
	}
	// Actually search through the log files for the file info
	textBytes := []byte(text)
	for _, log := range logs {
		fh, err := os.Open(log)
		if err != nil {
			// The log probably just doesn't exist yet because we're looking in the future, no big deal
			continue
		}
		// Use a byte scanner to find the text bytes in the log file
		scanner := bufio.NewScanner(fh)
		for scanner.Scan() {
			if bytes.Contains(scanner.Bytes(), textBytes) {
				fh.Close()
				return true
			}
		}
		fh.Close()
	}
	return false
}

func getDay(t time.Time) string {
	return fmt.Sprintf("%02d", t.Day())
}

func getMonth(t time.Time) string {
	return fmt.Sprintf("%02d", t.Month())
}

func getYear(t time.Time) string {
	return fmt.Sprintf("%04d", t.Year())
}
