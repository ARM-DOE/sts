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

type logMsg struct {
	output []interface{}
	stdout bool
	stderr bool
	toFile bool
}

// MakeDir creates the full path to a directory
type MakeDir func(path string, perm os.FileMode) (err error)

// OpenFile opens a file
type OpenFile func(path string, flag int, perm os.FileMode) (f *os.File, err error)

// FileIO implements sts.SendLogger and sts.ReceiveLogger
type FileIO struct {
	logger   *rollingFile
	logCh    chan string
	loggedCh chan bool
}

// NewFileIO creates a new FileIO logging instance
func NewFileIO(rootDir string, mkdir MakeDir, open OpenFile, keepInSync bool) *FileIO {
	f := &FileIO{
		logger:   newRollingFile(rootDir, "", 0, mkdir, open, keepInSync),
		logCh:    make(chan string),
		loggedCh: make(chan bool),
	}
	go func(f *FileIO) {
		for msg := range f.logCh {
			f.logger.log(msg)
			f.loggedCh <- true
		}
	}(f)
	return f
}

// Sent logs a file after it's been sent
func (f *FileIO) Sent(file sts.Sent) {
	f.logCh <- fmt.Sprintf(
		"%s:%s:%d:%d: %d ms",
		file.GetName(),
		file.GetHash(),
		file.GetSize(),
		time.Now().Unix(),
		file.TimeMs())
	<-f.loggedCh
}

// WasSent tries to find the path specified between the times specified
func (f *FileIO) WasSent(relPath string, after time.Time, before time.Time) bool {
	return f.logger.search(relPath+":", after, before)
}

// Received logs a file after it's been received
func (f *FileIO) Received(file sts.Received) {
	f.logCh <- fmt.Sprintf(
		"%s:%s:%s:%d:%d:",
		file.GetName(),
		file.GetRenamed(),
		file.GetHash(),
		file.GetSize(),
		time.Now().Unix())
	<-f.loggedCh
}

// WasReceived tries to find the path specified between the times specified
func (f *FileIO) WasReceived(relPath string, after time.Time, before time.Time) bool {
	return f.logger.search(relPath+":", after, before)
}

// Parse reads the log files in the provided time range and calls the handler
// on each record
func (f *FileIO) Parse(
	handler func(name, renamed, hash string, size int64, t time.Time) bool,
	after time.Time, before time.Time) bool {

	f.logger.eachLine(func(line string) bool {
		parts := strings.Split(line, ":")
		if len(parts) < 4 {
			return false
		}
		name := parts[0]
		renamed := ""
		i := 1
		if len(parts) > 4 {
			renamed = parts[i]
			i++
		}
		hash := parts[i]
		b, _ := strconv.ParseInt(parts[i+1], 10, 64)
		t, _ := strconv.ParseInt(parts[i+2], 10, 64)
		return handler(name, renamed, hash, b, time.Unix(t, 0))
	}, after, before)
	return false
}

// General is a rolling-file logger that implements sts.Logger
type General struct {
	logger    *rollingFile
	calldepth int
	debug     bool
	debugMux  sync.RWMutex
	logCh     chan logMsg
}

// NewGeneral creates a new General logging instance
func NewGeneral(rootDir string, debug bool, mkdir MakeDir, open OpenFile) *General {
	g := &General{
		logger:    newRollingFile(rootDir, "", log.Ldate|log.Ltime, mkdir, open, false),
		debug:     debug,
		debugMux:  sync.RWMutex{},
		calldepth: 1,
		logCh:     make(chan logMsg, 1000),
	}
	go func() {
		for msg := range g.logCh {
			if msg.toFile {
				g.logger.log(msg.output...)
			}
			if msg.stderr {
				fmt.Fprintln(os.Stderr, msg.output...)
			}
			if msg.stdout {
				fmt.Println(msg.output...)
			}
		}
	}()
	return g
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
			fmt.Sprintf("%s DEBUG %s:%d",
				time.Now().Format("2006-01-02 15:04:05"),
				filepath.Base(file),
				line),
		},
		params...)
	g.logCh <- logMsg{output: params, stdout: true}
}

// Info logs general information
func (g *General) Info(params ...interface{}) {
	params = append([]interface{}{"INFO"}, params...)
	g.logCh <- logMsg{output: params, stdout: true, toFile: true}
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
	g.logCh <- logMsg{output: params, stderr: true, toFile: true}
}

// RollingFile is the main struct for managing a rolling log file.
type rollingFile struct {
	keepInSync bool
	logger     *log.Logger
	root       string
	path       string
	mkdir      MakeDir
	open       OpenFile
	fh         *os.File
}

func newRollingFile(
	root, prefix string, flags int, mkdir MakeDir, open OpenFile, keepInSync bool,
) *rollingFile {
	if mkdir == nil {
		mkdir = os.MkdirAll
	}
	if open == nil {
		open = os.OpenFile
	}
	rf := &rollingFile{
		keepInSync: keepInSync,
		logger:     log.New(nil, prefix, flags),
		root:       root,
		mkdir:      mkdir,
		open:       open,
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
		rf.mkdir(filepath.Dir(path), os.ModePerm)
		rf.fh, err = rf.open(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("Failed to open log file: %s", err.Error()))
		}
		rf.logger.SetOutput(rf.fh)
	}
}

func (rf *rollingFile) log(t ...interface{}) {
	rf.rotate()
	rf.logger.Println(t...)
	if rf.keepInSync {
		// This can add significant overhead but is important when integrity of
		// the written file is necessary
		rf.fh.Sync()
	}
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
