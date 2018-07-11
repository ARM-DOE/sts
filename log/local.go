package log

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
func NewSend(rootDir string) *Send {
	return &Send{
		logger: newRollingFile(rootDir, "", 0),
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
	logger *rollingFile
	lock   sync.RWMutex
}

// NewReceive creates a new Receive logging instance
func NewReceive(rootDir string) *Receive {
	return &Receive{
		logger: newRollingFile(rootDir, "", 0),
		lock:   sync.RWMutex{},
	}
}

// Received logs a file after it's been received
func (r *Receive) Received(file sts.Received) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.logger.log(
		fmt.Sprintf(
			"%s:%s:%d:%d:",
			file.GetName(),
			file.GetHash(),
			file.GetSize(),
			time.Now().Unix()))
}

// WasReceived tries to find the path specified between the times specified
func (r *Receive) WasReceived(relPath string, after time.Time, before time.Time) bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.logger.search(relPath+":", after, before)
}

// Parse reads the log files in the provided time range and calls the handler
// on each record
func (r *Receive) Parse(
	handler func(name, hash string, size int64, t time.Time) bool,
	after time.Time, before time.Time) bool {

	r.lock.RLock()
	defer r.lock.RUnlock()
	r.logger.eachLine(func(line string) bool {
		parts := strings.Split(line, ":")
		if len(parts) < 4 {
			return false
		}
		b, _ := strconv.ParseInt(parts[2], 10, 64)
		t, _ := strconv.ParseInt(parts[3], 10, 64)
		return handler(parts[0], parts[1], b, time.Unix(t, 0))
	}, after, before)
	return false
}

// General is a rolling-file logger that implements sts.Logger
type General struct {
	logger    *rollingFile
	lock      sync.Mutex
	calldepth int
	debug     bool
	debugMux  sync.RWMutex
}

// NewGeneral creates a new General logging instance
func NewGeneral(rootDir string, debug bool) *General {
	return &General{
		logger:    newRollingFile(rootDir, "", log.Ldate|log.Ltime),
		lock:      sync.Mutex{},
		debug:     debug,
		debugMux:  sync.RWMutex{},
		calldepth: 1,
	}
}

func (g *General) setDebug(on bool) {
	g.debugMux.Lock()
	defer g.debugMux.Unlock()
	g.debug = on
}

func (g *General) getDebug() bool {
	g.debugMux.RLock()
	defer g.debugMux.RUnlock()
	return g.debug
}

// Debug logs debug messages (if set)
func (g *General) Debug(params ...interface{}) {
	if !g.getDebug() {
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

// each will loop over all log files in date range and call the provided
// handler for each one
func (rf *rollingFile) each(handler func(string) bool,
	start time.Time, stop time.Time) bool {

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
	for {
		path := rf.getPath(start)
		if handler(path) {
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

// eachLine will call the handler on each line of each log file in the input
// time range
func (rf *rollingFile) eachLine(handler func(string) bool,
	start, stop time.Time) bool {

	broke := rf.each(func(path string) bool {
		fp, err := os.Open(path)
		if err != nil {
			return false
		}
		defer fp.Close()
		scanner := bufio.NewScanner(fp)
		lines := 0
		for scanner.Scan() {
			if handler(scanner.Text()) {
				return true
			}
			lines++
		}
		return false
	}, start, stop)
	return broke
}

// search will look for a given text pattern in the log history
func (rf *rollingFile) search(text string,
	start time.Time, stop time.Time) bool {

	b := []byte(text)
	broke := rf.each(func(path string) bool {
		return fileutil.FindLine(path, b) != ""
	}, start, stop)
	return broke
}
