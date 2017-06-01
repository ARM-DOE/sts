package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts/fileutil"
)

// Out is the log directory for send log entries to match legacy STS.
const Out = "outgoing_to"

// In is the log directory for incoming log entries to match legacy STS.
const In = "incoming_from"

// Msg is the log directory for debug and error messages to match legacy STS.
const Msg = "messages"

var gLogLock sync.RWMutex
var gLoggers map[string]*Logger
var msgLogger *Logger

// Init will create a logger for each input "mode".
func Init(modes map[string]string, root string, debug bool) {
	gLogLock.Lock()
	defer gLogLock.Unlock()
	if gLoggers == nil {
		gLoggers = make(map[string]*Logger)
	}
	for mode, append := range modes {
		_, exists := gLoggers[mode]
		if !exists {
			if append == "" {
				append = mode
			}
			gLoggers[mode] = newLogger(filepath.Join(root, append), mode, debug)
		}
	}
	msgLogger = gLoggers[Msg] // For faster access.
}

// Done terminates logging for all created loggers.
func Done() {
	gLogLock.Lock()
	defer gLogLock.Unlock()
	if gLoggers == nil {
		return
	}
	for _, logger := range gLoggers {
		logger.close()
	}
}

func getLogger(mode string, prefix ...string) (*Logger, bool) {
	if len(prefix) > 0 {
		gLogLock.Lock()
		defer gLogLock.Unlock()
		key := mode + ":" + strings.Join(prefix, "-")
		if l, ok := gLoggers[key]; ok {
			return l, true
		}
		if l, ok := gLoggers[mode]; ok {
			gLoggers[key] = newLogger(filepath.Join(l.root, filepath.Join(prefix...)), l.mode, l.debug)
			return gLoggers[key], true
		}
		return nil, false
	}
	gLogLock.RLock()
	defer gLogLock.RUnlock()
	l, ok := gLoggers[mode]
	return l, ok
}

// Debug joins each item in params{} by a space, prepends a timestamp and the location of the message,
// and writes the result to stdout and to the "messages" log file.
func Debug(params ...interface{}) {
	if msgLogger == nil || !msgLogger.debug {
		return
	}
	_, file, line, _ := runtime.Caller(1) // Get file name and line number of the caller of this function
	params = append([]interface{}{fmt.Sprintf("DEBUG %s:%d", filepath.Base(file), line)}, params...)
	msgLogger.out(os.Stdout, params...)
	msgLogger.log(params...)
}

// Info joins each item in params{} by a space, prepends a timestamp and the location of the message,
// and writes the result to stdout and to the "messages" log file.
func Info(params ...interface{}) {
	if msgLogger == nil {
		fmt.Fprintln(os.Stdout, params...)
		return
	}
	_, file, line, _ := runtime.Caller(1) // Get file name and line number of the caller of this function
	params = append([]interface{}{fmt.Sprintf("INFO %s:%d", filepath.Base(file), line)}, params...)
	msgLogger.out(os.Stdout, params...)
	msgLogger.log(params...)
}

// Error joins each item in params{} by a space, prepends a timestamp and the location of the error,
// and writes the result to stderr and to the "messages" log file.
func Error(params ...interface{}) {
	if msgLogger == nil {
		fmt.Fprintln(os.Stderr, params...) // Prevent error message from being lost.
		return
	}
	_, file, line, _ := runtime.Caller(1) // Get file name and line number of the caller of this function
	params = append([]interface{}{fmt.Sprintf("ERROR %s:%d", filepath.Base(file), line)}, params...)
	msgLogger.out(os.Stderr, params...)
	msgLogger.log(params...)
}

// Sent writes a formatted string to the "outgoing_to" log file.
func Sent(msg string, hash string, size int64, ms int64, prefix ...string) {
	logger, exists := getLogger(Out, prefix...)
	if !exists {
		return
	}
	logger.log(fmt.Sprintf("%s:%s:%d:%d: %d ms", msg, hash, size, time.Now().Unix(), ms))
}

// FindSent looks for a file name in the outgoing log history.
func FindSent(text string, start time.Time, stop time.Time, prefix ...string) bool {
	if logger, ok := getLogger(Out, prefix...); ok {
		return logger.search(text+":", start, stop)
	}
	return false
}

// Received writes a formatted string to the "incoming_from" log file.
func Received(msg string, hash string, size int64, prefix ...string) {
	logger, exists := getLogger(In, prefix...)
	if !exists {
		return
	}
	logger.log(fmt.Sprintf("%s:%s:%d:%d:", msg, hash, size, time.Now().Unix()))
}

// FindReceived looks for a file name in the incoming log history.
func FindReceived(text string, start time.Time, stop time.Time, prefix ...string) bool {
	if logger, ok := getLogger(In, prefix...); ok {
		return logger.search(text+":", start, stop)
	}
	return false
}

// Logger is the main struct for managing a rolling log file.
type Logger struct {
	mode   string
	logger *log.Logger
	fh     *os.File
	lock   sync.RWMutex
	root   string
	path   string
	debug  bool
}

func newLogger(root, mode string, debug bool) *Logger {
	logger := &Logger{}
	logger.mode = mode
	logger.lock = sync.RWMutex{}
	logger.root = root
	logger.debug = debug
	if mode == Msg {
		if root != "" {
			logger.logger = log.New(nil, "", log.Ldate|log.Ltime)
		}
	} else {
		logger.logger = log.New(nil, "", 0)
	}
	return logger
}

func (logger *Logger) getPath(time time.Time) string {
	return filepath.Join(logger.root, getYear(time)+getMonth(time), getDay(time))
}

func (logger *Logger) getCurrPath() string {
	return logger.getPath(time.Now())
}

func (logger *Logger) rotate() {
	p := logger.getCurrPath()
	_, err := os.Stat(p)
	if logger.path != p || os.IsNotExist(err) || logger.fh == nil {
		logger.close()
		logger.path = p
		os.MkdirAll(filepath.Dir(p), os.ModePerm)
		logger.fh, err = os.OpenFile(p, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("Failed to open log file: %s", err.Error()))
		}
		logger.logger.SetOutput(logger.fh)
	}
}

func (logger *Logger) out(writer io.Writer, t ...interface{}) {
	logger.lock.Lock()
	defer logger.lock.Unlock()
	fmt.Fprintln(writer, t...)
}

func (logger *Logger) log(t ...interface{}) {
	logger.lock.Lock()
	defer logger.lock.Unlock()
	logger.rotate()
	logger.logger.Println(t...)
	logger.fh.Sync()
}

func (logger *Logger) close() {
	if logger.fh != nil {
		logger.fh.Close()
	}
}

// search will look for a given text pattern in the log history.
// If "start" is nonzero it will search from start to current.
// If "start" is zero it will search from current back nDays.
func (logger *Logger) search(text string, start time.Time, stop time.Time) bool {
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
		path := logger.getPath(start)
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
