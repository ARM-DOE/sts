package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/logging"
)

// DisabledName is the file name used to disable scanning externally.
const DisabledName = ".disabled"

type scanQueue struct {
	ScanTime int64                 `json:"time"`
	ScanDir  string                `json:"dir"`
	Files    map[string]*foundFile `json:"files"`
}

func newScanQueue(scanDir string) *scanQueue {
	q := &scanQueue{}
	q.ScanDir = scanDir
	q.Files = make(map[string]*foundFile)
	return q
}

func (q *scanQueue) add(path string) (f ScanFile, err error) {
	file, has := q.Files[path]
	if !has {
		file = &foundFile{}
		file.path = path
		q.Files[path] = file
	}
	if _, err = file.Reset(); err != nil {
		q.remove(path)
	}
	f = file
	return
}

func (q *scanQueue) remove(path string) {
	delete(q.Files, path)
}

func (q *scanQueue) load(path string) error {
	if path == "" {
		return nil
	}
	return fileutils.LoadJSON(path, q)
}

func (q *scanQueue) cache(path string) error {
	if path == "" {
		return nil
	}
	return fileutils.WriteJSON(path, q)
}

func (q *scanQueue) reset() {
	q.Files = make(map[string]*foundFile)
}

func (q *scanQueue) toRelPath(path string, nested int) string {
	base := strings.Replace(path, q.ScanDir+string(os.PathSeparator), "", 1)
	if nested == 0 {
		return base
	}
	parts := strings.Split(base, string(os.PathSeparator))
	if nested > len(parts) {
		return base
	}
	return strings.Join(parts[nested:], string(os.PathSeparator))
}

type foundFile struct {
	path    string
	relPath string
	Size    int64  `json:"size"`
	Time    int64  `json:"mtime"`
	Link    string `json:"p"`
	queued  bool
}

func (f *foundFile) GetPath(follow bool) string {
	if follow && f.Link != "" {
		return f.Link
	}
	return f.path
}

func (f *foundFile) GetRelPath() string {
	return f.relPath
}

func (f *foundFile) GetLinkedPath() string {
	if f.Link != "" {
		return f.Link
	}
	return f.path
}

func (f *foundFile) GetSize() int64 {
	return f.Size
}

func (f *foundFile) GetTime() int64 {
	return f.Time
}

func (f *foundFile) Reset() (changed bool, err error) {
	info, err := os.Stat(f.path)
	if err != nil {
		return
	}
	var size, time int64
	if info.Mode() == os.ModeSymlink {
		var p string
		var i os.FileInfo
		if p, err = os.Readlink(f.path); err != nil {
			return
		}
		if i, err = os.Stat(p); err != nil {
			return
		}
		if i.IsDir() {
			err = fmt.Errorf("Found sym link to directory")
			return
		}
		size = i.Size()
		if f.Link != p {
			changed = true
			f.Link = p
		}
	} else {
		size = info.Size()
		time = info.ModTime().Unix()
	}
	if size != f.Size || time != f.Time {
		changed = true
		f.Size = size
		f.Time = time
	}
	return
}

// ScanFileList is used for sorting ScanFiles.
type ScanFileList []ScanFile

func (f ScanFileList) Len() int { return len(f) }
func (f ScanFileList) Less(i, j int) bool {
	diff := f[i].GetTime() - f[j].GetTime()
	if diff == 0 {
		return f[i].GetRelPath() < f[j].GetRelPath()
	}
	return diff < 0
}
func (f ScanFileList) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

// Scanner is responsible for traversing a directory tree for files.
type Scanner struct {
	conf      *ScannerConf
	queue     *scanQueue
	aged      map[string]int
	cachePath string
}

// ScannerConf are the external configuration
type ScannerConf struct {
	ScanDir  string
	CacheDir string
	Delay    time.Duration
	MinAge   time.Duration
	MaxAge   time.Duration
	OutOnce  bool
	Nested   int
	Ignore   []string
}

// AddIgnore adds a pattern to the list that is used for filtering files from the scan list.
func (c *ScannerConf) AddIgnore(pattern string) {
	c.Ignore = append(c.Ignore, pattern)
}

// NewScanner returns a Scanner instance.
func NewScanner(conf *ScannerConf) *Scanner {
	scanner := &Scanner{}
	scanner.conf = conf
	scanner.aged = make(map[string]int)

	scanner.queue = newScanQueue(conf.ScanDir)

	if conf.CacheDir != "" {
		scanner.cachePath = filepath.Join(conf.CacheDir, toCacheName(conf.ScanDir))
		scanner.queue.load(scanner.cachePath)
	}

	conf.AddIgnore(`\.DS_Store$`)
	conf.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(fileutils.LockExt)))
	conf.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(DisabledName)))
	return scanner
}

// Start starts the daemon that puts files on the outChan and reads from the doneChan.
// If stopChan is nil, only scan once and trigger shutdown.
func (scanner *Scanner) Start(outChan chan<- []ScanFile, doneChan <-chan []DoneFile, stopChan <-chan bool) {
	force := true // Force the first scan.
	for {
		select {
		case <-stopChan:
			stopChan = nil // So we don't get here again.
		default:
			if outChan != nil && scanner.out(outChan, force) > 0 {
				force = false
				break
			}
			time.Sleep(time.Millisecond * 100) // So we don't thrash.
			break
		}
		// Important that we let at least one scan happen becuase if stopChan is passed
		// in nil, then we just want a single scan.
		if outChan != nil && stopChan == nil { // We only want to close a channel once.
			close(outChan)
			outChan = nil
			if doneChan == nil {
				return
			}
		}
		for {
			n := scanner.done(doneChan)
			if n < 0 {
				return // Channel closed; we're done.
			}
			if n > 0 {
				continue // Loop until we have no more.
			}
			break // Nothing to do.
		}
	}
}

func (scanner *Scanner) out(ch chan<- []ScanFile, force bool) int {
	// Make sure it's been long enough for another scan.
	if !force && time.Now().Sub(time.Unix(scanner.queue.ScanTime, 0)) < scanner.conf.Delay {
		return 0
	}

	files, success := scanner.scan()
	if !success || len(files) == 0 {
		return 0
	}

	// Send to outChan.
	select {
	case ch <- files:
		for _, file := range files {
			scanner.queue.Files[file.GetPath(false)].queued = true
		}
		return len(files)
	default:
		break
	}
	return 0
}

func (scanner *Scanner) done(ch <-chan []DoneFile) int {
	select {
	case doneFiles, ok := <-ch:
		if !ok {
			return -1
		}
		for _, doneFile := range doneFiles {
			logging.Debug("SCAN File Done:", doneFile.GetPath())
			scanner.queue.remove(doneFile.GetPath())
		}
		if scanner.conf.OutOnce {
			scanner.queue.cache(scanner.cachePath)
		}
		return len(doneFiles)
	default:
		break
	}
	return 0
}

// GetScanTime returns the last scan time.
func (scanner *Scanner) GetScanTime() time.Time {
	return time.Unix(scanner.queue.ScanTime, 0)
}

// GetScanFiles returns the cached list of files in the queue.
func (scanner *Scanner) GetScanFiles() ScanFileList {
	files := make(ScanFileList, 0)
	for path, info := range scanner.queue.Files {
		if scanner.conf.OutOnce && info.queued {
			continue
		}
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if scanner.conf.OutOnce {
				// Would be strange in a scenario where files are only queued once
				// for a file to disappear between its first scan and now.
				logging.Error("File disappeared:", path)
			}
			scanner.queue.remove(path)
			continue
		}
		files = append(files, &foundFile{
			path:    path,
			relPath: scanner.queue.toRelPath(path, scanner.conf.Nested),
			Size:    info.Size,
			Time:    info.Time,
			Link:    info.Link,
			queued:  false,
		})
	}
	sort.Sort(files)
	return files
}

// SetQueued is for manually marking files as being queued.  This way if code outside
// this component adds a file to the outgoing channel, it shouldn't happen a second
// time.
func (scanner *Scanner) SetQueued(path string) {
	if f, ok := scanner.queue.Files[path]; ok {
		logging.Debug("SCAN Set Queued:", path, f.queued)
		f.queued = true
	}
}

// Scan does a directory scan and returns a sorted list of found paths.
func (scanner *Scanner) scan() (ScanFileList, bool) {
	_, err := os.Stat(filepath.Join(scanner.queue.ScanDir, DisabledName))
	if err == nil {
		return nil, false // Found .disabled, don't scan.
	}
	start := time.Now().Unix()
	filepath.Walk(scanner.queue.ScanDir, scanner.handleNode)
	// Cache the queue (if we need to make sure files are only sent once).
	if scanner.conf.OutOnce {
		scanner.queue.ScanTime = start - int64(scanner.conf.MinAge.Seconds()) - 1
		scanner.queue.cache(scanner.cachePath)
	}
	return scanner.GetScanFiles(), true
}

func (scanner *Scanner) handleNode(path string, info os.FileInfo, err error) error {
	if info == nil || err != nil || info.IsDir() || scanner.shouldIgnore(path) {
		return nil
	}
	fTime := info.ModTime()
	fAge := time.Now().Sub(fTime)
	if fAge < scanner.conf.MinAge {
		return nil
	}
	if !scanner.conf.OutOnce || fTime.After(time.Unix(scanner.queue.ScanTime, 0)) {
		_, err = scanner.queue.add(path)
		if err != nil && scanner.conf.OutOnce {
			logging.Error("Failed to add file to queue:", err.Error())
			return nil
		}
		if scanner.conf.MaxAge > 0 {
			n := int(fAge / scanner.conf.MaxAge)
			if count, exists := scanner.aged[path]; n > 0 && (!exists || n > count) {
				logging.Error(fmt.Sprintf("Stale file found: %s (%dh)", path, int(fAge.Hours())))
				scanner.aged[path] = n
			}
		}
	}
	return nil
}

func (scanner *Scanner) shouldIgnore(path string) bool {
	relPath := scanner.queue.toRelPath(path, scanner.conf.Nested)
	for _, pattern := range scanner.conf.Ignore {
		matched, err := regexp.MatchString(pattern, relPath)
		if err != nil || matched {
			return true
		}
	}
	return false
}

func toCacheName(dir string) string {
	return fmt.Sprintf("%s.json", strings.Replace(strings.TrimRight(dir, string(os.PathSeparator)), string(os.PathSeparator), "--", -1))
}
