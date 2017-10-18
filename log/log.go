package log

import "code.arm.gov/dataflow/sts"

var std sts.Logger

// Init creates a single logger instance for general logging
func Init(rootDir string, debug bool) {
	if std != nil {
		if g, ok := std.(*General); ok && g.logger.root != rootDir {
			panic("Logger already initialized with a different path")
		}
		return
	}
	std = NewGeneral(rootDir, debug)
	std.(*General).calldepth = 2
}

// InitExternal sets the internal logger
func InitExternal(logger sts.Logger) {
	std = logger
}

func check() {
	if std == nil {
		panic("No logger defined")
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
