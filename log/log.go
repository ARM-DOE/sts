package log

import (
	"log"

	"github.com/arm-doe/sts"
)

var std sts.Logger

// Init creates a single logger instance for general logging
func Init(rootDir string, debug bool, mkdir MakeDir, open OpenFile) {
	if std != nil {
		if g, ok := std.(*General); ok && g.logger.root != rootDir {
			log.Fatalln("Logger already initialized with a different path")
		}
		return
	}
	std = NewGeneral(rootDir, debug, mkdir, open)
	std.(*General).calldepth = 2
}

// InitExternal sets the internal logger
func InitExternal(logger sts.Logger) {
	std = logger
}

// Get returns the standard initialized logger if it exists
func Get() sts.Logger {
	return std
}

// SetDebug sets debug mode to on or off
func SetDebug(on bool) {
	if logger, ok := std.(*General); ok {
		logger.setDebug(on)
	}
}

// GetDebug returns the current debug mode
func GetDebug() bool {
	if logger, ok := std.(*General); ok {
		return logger.getDebug()
	}
	return false
}

func check() {
	if std == nil {
		log.Fatalln("No logger defined")
	}
}

// Debug logs debug messages
func Debug(params ...interface{}) {
	check()
	std.Debug(params...)
}

// Info logs general information
func Info(params ...interface{}) {
	check()
	std.Info(params...)
}

// Error logs ...errors
func Error(params ...interface{}) {
	check()
	std.Error(params...)
}

// Recent gets N most recent messages (info and error) - max 1000
func Recent(n int) []string {
	check()
	return std.Recent(n)
}
