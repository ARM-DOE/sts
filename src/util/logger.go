package util

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const LOGGING_SEND = 1
const LOGGING_RECEIVE = 2
const LOGGING_DISK = 3
const LOGGING_ERROR = 4

// Logger contains data necessary to create the logging directory structure and write
// logs to files
type Logger struct {
	internal_logger *log.Logger
	file_handle     *os.File
	log_lock        sync.Mutex
	log_path        string
	base_path       string
	mode            int
}

// NewLogger creates a file with the specified mode and base path from the config.
// The mode changes the behavior of Logger.Log() - List of modes:
// 1 = send, 2 = receive, 3 = disk, 4 = error
func NewLogger(base_path string, mode int) Logger {
	new_logger := Logger{}
	new_logger.log_lock = sync.Mutex{}
	new_logger.mode = mode
	new_logger.base_path = base_path
	if mode == LOGGING_ERROR {
		new_logger.internal_logger = log.New(nil, "", log.Ldate|log.Ltime)
	} else {
		new_logger.internal_logger = log.New(nil, "", 0)
	}
	return new_logger
}

// updateFileHandle is called before each call to a Log() function. If the day has changed,
// or the file handler is nil (on startup), a new log file will be opened for appending.
// It takes an optional parameter host_names, which specifies whether or not to open a file with a
// host name in its path
func (logger *Logger) updateFileHandle(host_names ...string) {
	host_flag := false
	if len(host_names) > 0 {
		host_flag = true
	}
	var current_path string
	if host_flag {
		current_path = GetCurrentLogPath(logger.base_path, logger.mode, host_names[0])
	} else {
		current_path = GetCurrentLogPath(logger.base_path, logger.mode)
	}
	_, stat_err := os.Stat(current_path)
	if logger.log_path != current_path || os.IsNotExist(stat_err) || logger.file_handle == nil {
		logger.log_path = current_path
		logger.file_handle.Close()
		os.MkdirAll(filepath.Dir(current_path), os.ModePerm)
		var open_err error
		logger.file_handle, open_err = os.OpenFile(current_path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0700)
		if open_err != nil {
			fmt.Println("Couldn't open log file: ", open_err.Error())
		}
		logger.internal_logger.SetOutput(logger.file_handle)
	}
}

// LogError joins each item in params{} by a space, prepends a timestamp and the location of the error,
// and writes the result to stdout and to the "messages" log file.
func (logger *Logger) LogError(params ...interface{}) error {
	if logger.mode != LOGGING_ERROR {
		fmt.Println(params...) // Prevent error message from being lost.
		return errors.New(fmt.Sprintf("Can't call LogError with log of mode %d", logger.mode))
	}
	logger.log_lock.Lock()
	defer logger.log_lock.Unlock()
	logger.updateFileHandle()
	// Get file name and line number of the caller of this function
	_, file_name, line, _ := runtime.Caller(1)
	params = append([]interface{}{fmt.Sprintf("%s:%d", filepath.Base(file_name), line)}, params...)
	fmt.Println(params...)
	logger.internal_logger.Println(params...)
	return nil
}

// LogDisk writes a formatted string to the "to_disk" log file.
func (logger *Logger) LogDisk(s string, md5 string, size int64) {
	if logger.mode != LOGGING_DISK {
		panic(fmt.Sprintf("Can't call LogDisk with log of mode %d", logger.mode))
	}
	logger.log_lock.Lock()
	defer logger.log_lock.Unlock()
	logger.updateFileHandle()
	logger.internal_logger.Printf("%s:%s:%d:%d", s, md5, size, time.Now().Unix())
}

// LogSend writes a formatted string to the "outgoing_to" log file.
func (logger *Logger) LogSend(s string, md5 string, size int64, hostname string, send_time int64) {
	if logger.mode != LOGGING_SEND {
		panic(fmt.Sprintf("Can't call LogSend with log of mode %d", logger.mode))
	}
	logger.log_lock.Lock()
	defer logger.log_lock.Unlock()
	logger.updateFileHandle(hostname)
	log_string := fmt.Sprintf("%s:%s:%d:%d: %d ms", s, md5, size, time.Now().Unix(), send_time)
	logger.internal_logger.Printf(log_string)
}

// LogReceive writes a formatted string to the "incoming_from" log file.
func (logger *Logger) LogReceive(s string, md5 string, size int64, hostname string) {
	if logger.mode != LOGGING_RECEIVE {
		panic(fmt.Sprintf("Can't call LogReceive with log of mode %d", logger.mode))
	}
	logger.log_lock.Lock()
	defer logger.log_lock.Unlock()
	logger.updateFileHandle(hostname)
	log_string := fmt.Sprintf("%s:%s:%d:%d:", s, md5, size, time.Now().Unix())
	logger.internal_logger.Println(log_string)
	logger.file_handle.Sync()
}

// getCurrentLogPath returns what should be the path of the current log file. It is  used to
// detect when a day has passed, and a new log file should be opened. It takes optional argument host_name
// which must be of length 1 or not supplied. The given hostname will be appended to the log path.
func GetCurrentLogPath(base_path string, mode int, host_name ...string) string {
	now := time.Now()
	return GetLogPath(base_path, mode, now, host_name...)
}

func GetLogPath(base_path string, mode int, log_time time.Time, host_name ...string) string {
	if len(host_name) > 0 {
		return JoinPath(base_path, getDirectory(mode), host_name[0], getYear(log_time)+getMonth(log_time), getDay(log_time))
	}
	return JoinPath(base_path, getDirectory(mode), getYear(log_time)+getMonth(log_time), getDay(log_time))
}

func (logger *Logger) GetLogPath(log_time time.Time, host_name string) string {
	return GetLogPath(logger.base_path, logger.mode, log_time, host_name)
}

// getDirectory returns the preset directory name for a type of log.
func getDirectory(mode int) string {
	switch mode {
	case LOGGING_SEND:
		return "outgoing_to"
	case LOGGING_RECEIVE:
		return "incoming_from"
	case LOGGING_DISK:
		return "to_disk"
	case LOGGING_ERROR:
		return "messages"
	}
	panic(fmt.Sprintf("Unrecognized logging mode %d", mode))
	return "fail"
}

// getDay gets the current day (1-31) and pads it with a leading 0
// if it is less than 2 characters long. Ex. 9 becomes 09.
func getDay(log_time time.Time) string {
	day_string := strconv.Itoa(log_time.Day())
	if len(day_string) == 1 {
		return "0" + day_string
	}
	return day_string
}

// getMonth gets the current month (1-12) and pads it with a leading 0
// if it is less than 2 characters long. Ex. 9 becomes 09.
func getMonth(log_time time.Time) string {
	month_string := strconv.Itoa(int((log_time.Month())))
	if len(month_string) == 1 {
		return "0" + month_string
	}
	return month_string
}

// getYear gets the current 4-digit year.
func getYear(log_time time.Time) string {
	return strconv.Itoa(log_time.Year())
}
