package logging

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
)

// Out is the log directory for send log entries to match legacy STS.
const Out = "outgoing_to"

// In is the log directory for incoming log entries to match legacy STS.
const In = "incoming_from"

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
	params = append([]interface{}{fmt.Sprintf("ERROR %s:%d", filepath.Base(file), line)}, params...)
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
	logger, exists := gLoggers[Out]
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

// FindSent looks for a file name in the outgoing log history.
func FindSent(text string, start time.Time, stop time.Time, prefix ...string) bool {
	return GetLogger(Out).search(text+":", start, stop, prefix...)
}

// Received writes a formatted string to the "incoming_from" log file.
func Received(msg string, hash string, size int64, prefix ...string) {
	logger, exists := gLoggers[In]
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

// FindReceived looks for a file name in the incoming log history.
func FindReceived(text string, start time.Time, stop time.Time, prefix ...string) bool {
	return GetLogger(In).search(text+":", start, stop, prefix...)
}

func getPath(basePath string, mode string, time time.Time, prefix ...string) string {
	root := filepath.Join(basePath, mode)
	if len(prefix) > 0 {
		root = filepath.Join(root, filepath.Join(prefix...))
	}
	return filepath.Join(root, getYear(time)+getMonth(time), getDay(time))
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

// search will look for a given text pattern in the log history.
// If "start" is nonzero it will search from start to current.
// If "start" is zero it will search from current back nDays.
func (logger *Logger) search(text string, start time.Time, stop time.Time, prefix ...string) bool {
	logger.lock.RLock()
	defer logger.lock.RUnlock()
	if start.IsZero() {
		start = time.Now()
	}
	if stop.IsZero() {
		stop = time.Now()
	}
	if start.Equal(stop) {
		return false
	}
	offset := time.Duration(24 * time.Hour)
	if stop.Before(start) {
		offset *= -1
	}
	b := []byte(text)
	for {
		path := logger.getPath(start, prefix...)
		if fileutils.FindLine(path, b) != "" {
			return true
		}
		if offset > 0 && start.After(stop) {
			break
		}
		if offset < 0 && start.Before(stop) {
			break
		}
		start = start.Add(offset)
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
