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
	"github.com/ARM-DOE/sts/pathutils"
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
	base := strings.Replace(path, q.ScanDir+pathutils.Sep, "", 1)
	if nested == 0 {
		return base
	}
	parts := strings.Split(base, pathutils.Sep)
	if nested > len(parts) {
		return base
	}
	return strings.Join(parts[nested:], pathutils.Sep)
}

type foundFile struct {
	path    string
	relPath string
	Size    int64 // Public so it can get persisted
	Time    int64 // Public so it can get persisted
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

	scanner.cachePath = pathutils.Join(cacheDir, toCacheName(scanDir))
	scanner.queue.load(scanner.cachePath)

	scanner.AddIgnore(`\.DS_Store$`)
	return scanner
}

// AddIgnore adds a pattern to the list that is used for filtering files from the scan list.
func (scanner *Scanner) AddIgnore(pattern string) {
	scanner.ignore = append(scanner.ignore, pattern)
}

// Start starts the daemon that puts files on the outChan and reads from the doneChan.
func (scanner *Scanner) Start(outChan chan []ScanFile, doneChan chan DoneFile) {
	for {
		for {
			select {
			case doneFile := <-doneChan:
				logging.Debug("SCAN Done:", doneFile.GetSortFile().GetPath())
				scanner.queue.remove(doneFile.GetSortFile().GetPath())
				continue
			default:
				break
			}
			time.Sleep(time.Millisecond * 100) // So we don't thrash.
			break
		}

		// Make sure it's been long enough for another scan.
		if scanner.queue.ScanTime > time.Now().Unix()-int64(scanner.delay.Seconds()) {
			continue
		}

		start := time.Now().Unix()
		if !scanner.scan() || len(scanner.queue.Files) == 0 {
			continue
		}
		scanner.queue.ScanTime = start - int64(scanner.minAge.Seconds()) - 1

		// Sort files by modtime (oldest first).
		files := make(ScanFileList, 0)
		for path, info := range scanner.queue.Files {
			if info.queued {
				continue
			}
			files = append(files, &foundFile{path, scanner.queue.toRelPath(path, scanner.nested), info.Size, info.Time, false})
		}
		if len(files) == 0 {
			continue
		}
		sort.Sort(files)

		logging.Debug(fmt.Sprintf("SCAN Found %d Files", len(files)))

		// Send to outChan.
		select {
		case outChan <- files:
			for _, file := range files {
				scanner.queue.Files[file.GetPath()].queued = true
			}
			scanner.queue.reset()
		default:
			break
		}

		// Cache the queue (if we need to make sure files are only sent once)
		if scanner.outOnce {
			scanner.queue.cache(scanner.cachePath)
		}
	}
}

func (scanner *Scanner) scan() bool {
	_, err := os.Stat(pathutils.Join(scanner.queue.ScanDir, DisabledName))
	if err == nil {
		return false // Found .disabled, don't scan.
	}
	filepath.Walk(scanner.queue.ScanDir, scanner.handleNode)
	return true
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
	for _, pattern := range scanner.ignore {
		matched, err := regexp.MatchString(pattern, path)
		if err != nil || matched {
			return true
		}
	}
	return false
}

func toCacheName(dir string) string {
	return fmt.Sprintf("%s.json", strings.Replace(strings.TrimRight(dir, pathutils.Sep), pathutils.Sep, "_", -1))
}

func fromCacheName(name string) string {
	return strings.TrimRight(strings.Replace(name, "_", pathutils.Sep, -1), ".json")
}
