package scan

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/logging"
)

// DisabledName is the file name used to disable scanning externally.
const DisabledName = ".disabled"

type scanCache struct {
	nested   int
	scanTime int64
	dirty    bool
	LastTime int64                 `json:"time"`
	ScanDir  string                `json:"dir"`
	Files    map[string]*foundFile `json:"files"`
}

func newScanCache(scanDir string, nested int) *scanCache {
	q := &scanCache{}
	q.nested = nested
	q.ScanDir = scanDir
	q.Files = make(map[string]*foundFile)
	return q
}

func (q *scanCache) add(path string, info os.FileInfo) (f sts.ScanFile, err error) {
	nesting, relPath := q.parsePath(path)
	key := q.toKey(nesting, relPath)
	file, has := q.Files[key]
	if !has {
		file = &foundFile{q: q}
		file.path = path
		file.relPath = relPath
		file.Nesting = nesting
		q.Files[key] = file
	}
	changed, err := file.reset(info)
	if err != nil {
		q.remove(key)
		return
	}
	if changed {
		file.queued = false
		file.Done = false
		q.dirty = true
	}
	f = file
	return
}

func (q *scanCache) get(path string) (f *foundFile, ok bool) {
	key := q.toKey(q.parsePath(path))
	f, ok = q.Files[key]
	return
}

func (q *scanCache) remove(key string) {
	if _, ok := q.Files[key]; ok {
		delete(q.Files, key)
		q.dirty = true
	}
}

func (q *scanCache) done(path string) {
	key := q.toKey(q.parsePath(path))
	if f, ok := q.Files[key]; ok {
		f.Done = true
		q.dirty = true
	}
}

func (q *scanCache) load(path string) (err error) {
	if path == "" {
		return nil
	}
	if err = fileutil.LoadJSON(path, q); err != nil && !os.IsNotExist(err) {
		return err
	}
	err = nil
	for k, f := range q.Files {
		f.q = q
		f.path = q.toFullPath(f.Nesting, k)
		if f.Nesting == "" {
			f.relPath = k
		} else {
			_, f.relPath = q.parsePath(f.path)
		}
	}
	return
}

func (q *scanCache) cache(path string) (err error) {
	if path == "" {
		return nil
	}
	if err = fileutil.WriteJSON(path, q); err != nil {
		return
	}
	q.dirty = false
	return
}

func (q *scanCache) reset() {
	q.Files = make(map[string]*foundFile)
	q.scanTime = 0
	q.LastTime = 0
}

func (q *scanCache) parsePath(path string) (nesting string, relPath string) {
	relPath = strings.Replace(path, q.ScanDir+string(os.PathSeparator), "", 1)
	if q.nested == 0 {
		return
	}
	parts := strings.Split(relPath, string(os.PathSeparator))
	if q.nested > len(parts) {
		return
	}
	nesting = strings.Join(parts[:q.nested], string(os.PathSeparator))
	relPath = strings.Join(parts[q.nested:], string(os.PathSeparator))
	return
}

func (q *scanCache) toFullPath(nesting string, relPath string) string {
	if nesting != "" {
		return filepath.Join(q.ScanDir, nesting, relPath)
	}
	return filepath.Join(q.ScanDir, relPath)
}

func (q *scanCache) toKey(nesting string, relPath string) string {
	if nesting != "" {
		return filepath.Join(nesting, relPath)
	}
	return relPath
}

type foundFile struct {
	q       *scanCache
	path    string
	relPath string
	Size    int64  `json:"size"`
	Time    int64  `json:"mtime"`
	Link    string `json:"p"`
	Nesting string `json:"n"`
	Done    bool   `json:"done"`
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

func (f *foundFile) reset(info os.FileInfo) (changed bool, err error) {
	if info == nil {
		info, err = os.Lstat(f.path)
		if err != nil {
			return
		}
	}
	var size, time int64
	if info.Mode()&os.ModeSymlink != 0 {
		var p string
		var i os.FileInfo
		if p, err = os.Readlink(f.path); err != nil {
			return
		}
		if i, err = os.Stat(p); err != nil {
			return
		}
		if i.IsDir() {
			err = fmt.Errorf("Found symbolic link to directory")
			return
		}
		size = i.Size()
		time = i.ModTime().Unix()
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
	if changed {
		f.q.dirty = true
	}
	return
}

func (f *foundFile) Reset() (bool, error) {
	return f.reset(nil)
}

// FileList is for being able to sort a slice of ScanFile references.
type FileList []sts.ScanFile

func (f FileList) Len() int { return len(f) }
func (f FileList) Less(i, j int) bool {
	diff := f[i].GetTime() - f[j].GetTime()
	if diff == 0 {
		return f[i].GetRelPath() < f[j].GetRelPath()
	}
	return diff < 0
}
func (f FileList) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

// Scanner is responsible for traversing a directory tree for files.
type Scanner struct {
	Conf      *ScannerConf
	cache     *scanCache
	aged      map[string]int
	cachePath string
}

// ScannerConf is the struct for external configuration.
type ScannerConf struct {
	ScanDir        string
	CacheDir       string
	CacheAge       time.Duration
	Delay          time.Duration
	MinAge         time.Duration
	MaxAge         time.Duration
	OutOnce        bool
	Nested         int
	Include        []*regexp.Regexp
	Ignore         []*regexp.Regexp
	FollowSymlinks bool
	ZeroError      bool // Log error for zero-length files
}

// AddInclude adds a pattern to the list that is used for including files in the scan list.
func (c *ScannerConf) AddInclude(pattern *regexp.Regexp) {
	c.Include = append(c.Include, pattern)
}

// AddIgnore adds a pattern to the list that is used for filtering files from the scan list.
func (c *ScannerConf) AddIgnore(pattern *regexp.Regexp) {
	c.Ignore = append(c.Ignore, pattern)
}

// AddIgnoreString calls AddIgnore after compiling the provided string to a regular expression.
func (c *ScannerConf) AddIgnoreString(pattern string) {
	c.AddIgnore(regexp.MustCompile(pattern))
}

// NewScanner returns a Scanner instance.
func NewScanner(conf *ScannerConf) (*Scanner, error) {
	scanner := &Scanner{}
	scanner.Conf = conf
	scanner.aged = make(map[string]int)

	scanner.cache = newScanCache(conf.ScanDir, conf.Nested)

	if conf.CacheDir != "" {
		scanner.cachePath = filepath.Join(conf.CacheDir, toCacheName(conf.ScanDir))
		if err := scanner.cache.load(scanner.cachePath); err != nil {
			return scanner, err
		}
	}

	conf.AddIgnoreString(`\.DS_Store$`)
	conf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(fileutil.LockExt)))
	conf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(DisabledName)))
	return scanner, nil
}

// Start starts the daemon that puts files on the outChan and reads from the doneChan.
// If stopChan is nil, only scan once and trigger shutdown.
func (scanner *Scanner) Start(outChan chan<- []sts.ScanFile, doneChan <-chan []sts.DoneFile, stopChan <-chan bool) {
	for {
		select {
		case <-stopChan:
			stopChan = nil // So we don't get here again.
		default:
			if outChan != nil {
				scanner.out(outChan, false)
			}
			time.Sleep(time.Millisecond * 100) // So we don't thrash.
		}
		// Important that we let at least one scan happen because if stopChan is passed
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

func (scanner *Scanner) out(ch chan<- []sts.ScanFile, force bool) int {
	scanner.writeCache()

	// Make sure it's been long enough for another scan.
	scanned := scanner.cache.scanTime
	if !force && time.Now().Sub(time.Unix(scanned, 0)) < scanner.Conf.Delay {
		return 0
	}

	scanner.pruneCache()

	files, success := scanner.Scan()
	if !success || len(files) == 0 {
		return 0
	}

	scanner.writeCache()

	// Send to outChan.
	select {
	case ch <- files:
		for _, file := range files {
			file.(*foundFile).queued = true
		}
		return len(files)
	default:
		break
	}
	return 0
}

func (scanner *Scanner) done(ch <-chan []sts.DoneFile) int {
	if ch == nil {
		return 0
	}
	select {
	case doneFiles, ok := <-ch:
		if !ok {
			return -1
		}
		for _, doneFile := range doneFiles {
			if doneFile.GetSuccess() {
				logging.Debug("SCAN File Done:", doneFile.GetRelPath())
				scanner.cache.done(doneFile.GetPath())
				continue
			}
			if f, ok := scanner.cache.get(doneFile.GetPath()); ok {
				logging.Debug("SCAN File Failed:", doneFile.GetPath())
				f.queued = false
			}
		}
		scanner.writeCache()
		return len(doneFiles)
	default:
		break
	}
	return 0
}

func (scanner *Scanner) pruneCache() {
	if !scanner.Conf.OutOnce {
		return
	}
	logging.Debug("SCAN Pruning Cache:", scanner.Conf.ScanDir)
	for key, file := range scanner.cache.Files {
		// Only remove files from the cache that have finished and that are older
		// than the cache boundary time, which is the start time of the previous
		// scan minus the minimum file age minus the configured cache age (to
		// avoid potentially sending a file more than once).
		if file.Done && file.GetTime() < scanner.cache.LastTime {
			scanner.cache.remove(key)
		}
	}
}

func (scanner *Scanner) writeCache() {
	if scanner.cachePath != "" && scanner.Conf.OutOnce && scanner.cache.dirty {
		logging.Debug("SCAN Writing Cache:", scanner.Conf.ScanDir)
		scanner.cache.cache(scanner.cachePath)
	}
}

// GetScanFiles returns the list of files in the cache.
func (scanner *Scanner) GetScanFiles() FileList {
	files := make(FileList, 0)
	for k, f := range scanner.cache.Files {
		if scanner.Conf.OutOnce {
			if f.queued || f.Done {
				continue
			}
		}
		if _, err := os.Lstat(f.path); os.IsNotExist(err) {
			if scanner.Conf.OutOnce {
				// This can happen (legitimately) if a crash occurs after a file
				// is cleaned following a successful transfer but before the scanner
				// is notified to remove the file from the list.  In this case,
				// we'll get here on the restart and the error is benign.
				logging.Error("File disappeared:", f.path)
			}
			scanner.cache.remove(k)
			continue
		}
		files = append(files, f)
	}
	sort.Sort(files)
	return files
}

// Reset removes any existing cache entries and resets the time bounds.
func (scanner *Scanner) Reset() {
	scanner.cache.reset()
}

// SetQueued is for manually marking files as being queued.  This way if code outside
// this component adds a file to the outgoing channel, it shouldn't happen a second
// time.
func (scanner *Scanner) SetQueued(path string) {
	if f, ok := scanner.cache.get(path); ok {
		logging.Debug("SCAN Set Queued:", f.path)
		f.queued = true
	}
}

// Scan does a directory scan and returns a sorted list of found paths.
func (scanner *Scanner) Scan() (FileList, bool) {
	_, err := os.Lstat(filepath.Join(scanner.cache.ScanDir, DisabledName))
	if err == nil {
		return nil, false // Found .disabled, don't scan.
	}
	logging.Debug("SCAN Scanning...")
	scanner.cache.scanTime = time.Now().Unix()
	if err := fileutil.Walk(scanner.cache.ScanDir, scanner.handleNode, scanner.Conf.FollowSymlinks); err != nil {
		logging.Error(err.Error())
	} else {
		scanner.cache.LastTime = scanner.cache.scanTime -
			int64(scanner.Conf.MinAge.Seconds()) -
			int64(scanner.Conf.CacheAge.Seconds())
	}
	return scanner.GetScanFiles(), true
}

func (scanner *Scanner) handleNode(path string, info os.FileInfo, err error) error {
	if info == nil || err != nil {
		return err
	}
	nesting, relPath := scanner.cache.parsePath(path)
	if info.IsDir() {
		if scanner.shouldIgnore(relPath, true) {
			// Best to skip entire directories if they match an ignore pattern.
			return filepath.SkipDir
		}
		return nil
	}
	if scanner.shouldIgnore(relPath, false) {
		return nil
	}
	fTime := info.ModTime()
	fAge := time.Now().Sub(fTime)
	if fAge < scanner.Conf.MinAge {
		return nil
	}
	if scanner.Conf.MaxAge > 0 {
		n := int(fAge / scanner.Conf.MaxAge)
		if count, exists := scanner.aged[path]; n > 0 && (!exists || n > count) {
			logging.Error(fmt.Sprintf("Stale file found: %s (%dh)", path, int(fAge.Hours())))
			scanner.aged[path] = n
		}
	}
	if info.Size() == 0 {
		if scanner.Conf.ZeroError {
			logging.Error(fmt.Sprintf("Zero-length file found: %s", path))
		}
		return nil
	}
	if !scanner.Conf.OutOnce {
		scanner.cache.add(path, info)
		return nil
	}
	key := scanner.cache.toKey(nesting, relPath)
	// If this file is in the cache and hasn't changed, skip it.
	if f, ok := scanner.cache.Files[key]; ok && fTime.Unix() == f.Time && info.Size() == f.Size {
		return nil
	}
	// If this file has a mod time before the cache boundary time, skip it.
	if fTime.Unix() < scanner.cache.LastTime {
		return nil
	}
	logging.Debug("SCAN Found:", path)
	if _, err = scanner.cache.add(path, info); err != nil {
		logging.Error("Failed to add file to queue:", path, "->", err.Error())
		return nil
	}
	return nil
}

func (scanner *Scanner) shouldIgnore(relPath string, isDir bool) bool {
	var pattern *regexp.Regexp
	if !isDir && len(scanner.Conf.Include) > 0 {
		for _, pattern = range scanner.Conf.Include {
			if pattern.MatchString(relPath) {
				return false
			}
		}
		return true
	}
	for _, pattern = range scanner.Conf.Ignore {
		if pattern.MatchString(relPath) {
			return true
		}
	}
	return false
}

func toCacheName(dir string) string {
	return fmt.Sprintf("%s.json", fileutil.StringMD5(dir))
}
