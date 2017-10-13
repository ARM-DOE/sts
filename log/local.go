package log

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
)

// Send implements sts.SendLogger
type Send struct {
	logger *rollingFile
	lock   sync.RWMutex
}

// NewSend creates a new Send logging instance
func NewSend(rootDir string, host string) *Send {
	return &Send{
		logger: newRollingFile(filepath.Join(rootDir, host), "", 0),
		lock:   sync.RWMutex{},
	}
}

// Sent logs a file after it's been sent
func (s *Send) Sent(file sts.Sent) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.logger.log(
		fmt.Sprintf(
			"%s:%s:%d:%d: %d ms",
			file.GetName(),
			file.GetHash(),
			file.GetSize(),
			time.Now().Unix(),
			file.TimeMs()))
}

// WasSent tries to find the path specified between the times specified
func (s *Send) WasSent(relPath string, after time.Time, before time.Time) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.logger.search(relPath+":", after, before)
}

// Receive implements sts.ReceiveLogger
type Receive struct {
	rootDir string
	lock    *sync.Mutex
	loggers map[string]*rollingFile
	locks   map[string]*sync.RWMutex
}

// NewReceive creates a new Receive logging instance
func NewReceive(rootDir string) *Receive {
	return &Receive{
		rootDir: rootDir,
		lock:    &sync.Mutex{},
		loggers: make(map[string]*rollingFile),
		locks:   make(map[string]*sync.RWMutex),
	}
}

func (r *Receive) bySource(source string) (logger *rollingFile, lock *sync.RWMutex) {
	r.lock.Lock()
	defer r.lock.Unlock()
	var ok bool
	if logger, ok = r.loggers[source]; !ok {
		logger = newRollingFile(filepath.Join(r.rootDir, source), "", 0)
		r.loggers[source] = logger
	}
	if lock, ok = r.locks[source]; !ok {
		lock = &sync.RWMutex{}
		r.locks[source] = lock
	}
	return
}

// Received logs a file after it's been fully received
func (r *Receive) Received(source string, file sts.Received) {
	logger, lock := r.bySource(source)
	lock.Lock()
	defer lock.Unlock()
	logger.log(
		fmt.Sprintf(
			"%s:%s:%d:%d:",
			file.GetName(),
			file.GetHash(),
			file.GetSize(),
			time.Now().Unix()))
}

// WasReceived tries to find the path specified between the times specified
func (r *Receive) WasReceived(source string, relPath string, after time.Time, before time.Time) bool {
	logger, lock := r.bySource(source)
	lock.RLock()
	defer lock.RUnlock()
	return logger.search(relPath+":", after, before)
}

// General is a rolling-file logger that implements sts.Logger
type General struct {
	logger    *rollingFile
	lock      sync.Mutex
	calldepth int
	debug     bool
}

// NewGeneral creates a new General logging instance
func NewGeneral(rootDir string, debug bool) *General {
	return &General{
		logger:    newRollingFile(rootDir, "", log.Ldate|log.Ltime),
		lock:      sync.Mutex{},
		debug:     debug,
		calldepth: 1,
	}
}

// Debug logs debug messages (if set)
func (g *General) Debug(params ...interface{}) {
	if !g.debug {
		return
	}
	_, file, line, ok := runtime.Caller(g.calldepth)
	if !ok {
		file = "???"
		line = 0
	}
	params = append(
		[]interface{}{
			fmt.Sprintf("DEBUG %s:%d", filepath.Base(file), line),
		},
		params...)
	g.lock.Lock()
	defer g.lock.Unlock()
	fmt.Println(params...)
	g.logger.log(params...)
}

// Info logs general information
func (g *General) Info(params ...interface{}) {
	params = append([]interface{}{"INFO"}, params...)
	g.lock.Lock()
	defer g.lock.Unlock()
	fmt.Println(params...)
	g.logger.log(params...)
}

// Error logs errors
func (g *General) Error(params ...interface{}) {
	_, file, line, ok := runtime.Caller(g.calldepth)
	if !ok {
		file = "???"
		line = 0
	}
	params = append(
		[]interface{}{
			fmt.Sprintf("ERROR %s:%d", filepath.Base(file), line),
		},
		params...)
	g.lock.Lock()
	defer g.lock.Unlock()
	fmt.Fprintln(os.Stderr, params...)
	g.logger.log(params...)
}

// RollingFile is the main struct for managing a rolling log file.
type rollingFile struct {
	logger *log.Logger
	root   string
	path   string
	fh     *os.File
}

func newRollingFile(root, prefix string, flags int) *rollingFile {
	rf := &rollingFile{
		logger: log.New(nil, prefix, flags),
		root:   root,
	}
	return rf
}

func (rf *rollingFile) getPath(time time.Time) string {
	return filepath.Join(
		rf.root,
		fmt.Sprintf("%04d%02d", time.Year(), time.Month()),
		fmt.Sprintf("%02d", time.Day()))
}

func (rf *rollingFile) getCurrPath() string {
	return rf.getPath(time.Now())
}

func (rf *rollingFile) rotate() {
	path := rf.getCurrPath()
	_, err := os.Stat(path)
	if rf.path != path || os.IsNotExist(err) || rf.fh == nil {
		rf.close()
		rf.path = path
		os.MkdirAll(filepath.Dir(path), os.ModePerm)
		rf.fh, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("Failed to open log file: %s", err.Error()))
		}
		rf.logger.SetOutput(rf.fh)
	}
}

func (rf *rollingFile) log(t ...interface{}) {
	rf.rotate()
	rf.logger.Println(t...)
	rf.fh.Sync()
}

func (rf *rollingFile) close() {
	if rf.fh != nil {
		rf.fh.Close()
	}
}

// search will look for a given text pattern in the log history
func (rf *rollingFile) search(text string, start time.Time, stop time.Time) bool {
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
		path := rf.getPath(start)
		if fileutil.FindLine(path, b) != "" {
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
