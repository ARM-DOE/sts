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

func (q *scanQueue) add(path string, size int64, time int64) ScanFile {
	file, has := q.Files[path]
	if !has {
		file = &foundFile{}
		file.path = path
		q.Files[path] = file
	}
	file.Size = size
	file.Time = time
	return file
}

func (q *scanQueue) remove(path string) {
	delete(q.Files, path)
}

func (q *scanQueue) load(path string) error {
	return fileutils.LoadJSON(path, q)
}

func (q *scanQueue) cache(path string) error {
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
	Size    int64 `json:"size"`
	Time    int64 `json:"mtime"`
	queued  bool
}

func (f *foundFile) GetPath() string {
	return f.path
}

func (f *foundFile) GetRelPath() string {
	return f.relPath
}

func (f *foundFile) GetSize() int64 {
	return f.Size
}

func (f *foundFile) GetTime() int64 {
	return f.Time
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
	delay   time.Duration
	minAge  time.Duration
	outOnce bool
	nested  int
	ignore  []string

	queue     *scanQueue
	cachePath string
}

// NewScanner returns a Scanner instance.
func NewScanner(scanDir string, cacheDir string, scanDelay time.Duration, minAge time.Duration, outOnce bool, nested int) *Scanner {
	scanner := &Scanner{}

	scanner.delay = scanDelay
	scanner.outOnce = outOnce
	scanner.minAge = minAge
	scanner.nested = nested // The number of path "elements" after the scanDir to start the "relPath"
	scanner.ignore = make([]string, 0)

	scanner.queue = newScanQueue(scanDir)

	scanner.cachePath = filepath.Join(cacheDir, toCacheName(scanDir))
	scanner.queue.load(scanner.cachePath)

	scanner.AddIgnore(`\.DS_Store$`)
	scanner.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(fileutils.LockExt)))
	scanner.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(DisabledName)))
	return scanner
}

// AddIgnore adds a pattern to the list that is used for filtering files from the scan list.
func (scanner *Scanner) AddIgnore(pattern string) {
	scanner.ignore = append(scanner.ignore, pattern)
}

// Start starts the daemon that puts files on the outChan and reads from the doneChan.
func (scanner *Scanner) Start(outChan chan []ScanFile, doneChan chan []DoneFile) {
	for {
		ndone := 0
		for {
			select {
			case doneFiles := <-doneChan:
				for _, doneFile := range doneFiles {
					logging.Debug("SCAN File Done:", doneFile.GetPath())
					scanner.queue.remove(doneFile.GetPath())
					ndone++
				}
				continue
			default:
				break
			}
			if ndone > 0 && scanner.outOnce {
				scanner.queue.cache(scanner.cachePath)
			}
			time.Sleep(time.Millisecond * 100) // So we don't thrash.
			break
		}

		// Make sure it's been long enough for another scan.
		if scanner.queue.ScanTime > time.Now().Unix()-int64(scanner.delay.Seconds()) {
			continue
		}

		files, success := scanner.scan()
		if !success || len(files) == 0 {
			continue
		}

		// Send to outChan.
		select {
		case outChan <- files:
			for _, file := range files {
				scanner.queue.Files[file.GetPath()].queued = true
			}
		default:
			break
		}
	}
}

// GetScanTime returns the last scan time.
func (scanner *Scanner) GetScanTime() time.Time {
	return time.Unix(scanner.queue.ScanTime, 0)
}

// GetScanFiles returns the cached list of files in the queue.
func (scanner *Scanner) GetScanFiles() ScanFileList {
	files := make(ScanFileList, 0)
	for path, info := range scanner.queue.Files {
		if scanner.outOnce && info.queued {
			continue
		}
		if _, err := os.Stat(path); os.IsNotExist(err) {
			logging.Error("File disappeared:", path)
			scanner.queue.remove(path)
			continue
		}
		files = append(files, &foundFile{
			path:    path,
			relPath: scanner.queue.toRelPath(path, scanner.nested),
			Size:    info.Size,
			Time:    info.Time,
			queued:  false,
		})
	}
	sort.Sort(files)
	return files
}

// SetQueued is for manually marking files as being queued.  This way if code outside this component adds
// a file to the outgoing channel, it shouldn't happen a second time.
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
	// Cache the queue (if we need to make sure files are only sent once)
	if scanner.outOnce {
		scanner.queue.ScanTime = start - int64(scanner.minAge.Seconds()) - 1
		scanner.queue.cache(scanner.cachePath)
	}
	return scanner.GetScanFiles(), true
}

func (scanner *Scanner) handleNode(path string, info os.FileInfo, err error) error {
	if info == nil || info.IsDir() || scanner.shouldIgnore(path) {
		return nil
	}
	fTime := info.ModTime()
	fSize := info.Size()
	if fTime.Unix() < (time.Now().Unix() - int64(scanner.minAge.Seconds())) {
		if !scanner.outOnce || fTime.After(time.Unix(scanner.queue.ScanTime, 0)) {
			scanner.queue.add(path, fSize, fTime.Unix())
		}
	}
	return nil
}

func (scanner *Scanner) shouldIgnore(path string) bool {
	relPath := scanner.queue.toRelPath(path, scanner.nested)
	for _, pattern := range scanner.ignore {
		matched, err := regexp.MatchString(pattern, relPath)
		if err != nil || matched {
			return true
		}
	}
	return false
}

func toCacheName(dir string) string {
	return fmt.Sprintf("%s.json", strings.Replace(strings.TrimRight(dir, string(os.PathSeparator)), string(os.PathSeparator), "_", -1))
}

// Not used currently but might come in handy sometime.
// func fromCacheName(name string) string {
// 	return strings.TrimRight(strings.Replace(name, "_", string(os.PathSeparator), -1), ".json")
// }
