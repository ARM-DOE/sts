package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/log"
)

// disabledName is the file name used to disable scanning externally.
const disabledName = ".disabled"

type localFile struct {
	path    string
	name    string
	info    os.FileInfo
	meta    *meta
	metaEnc []byte
}

func newLocalFile(path string, relPath string, info os.FileInfo) (f *localFile, err error) {
	f = &localFile{
		path: path,
		name: relPath,
		info: info,
		meta: &meta{},
	}
	if info.Mode()&os.ModeSymlink != 0 {
		var linkedPath string
		if linkedPath, err = os.Readlink(path); err != nil {
			return
		}
		if f.info, err = os.Stat(linkedPath); err != nil {
			return
		}
		if f.info.IsDir() {
			err = filepath.SkipDir
			return
		}
		f.meta.Link = linkedPath
		f.metaEnc, err = encodeMeta(f.meta)
		if err != nil {
			return
		}
	}
	return
}

func (f *localFile) GetPath() string {
	return f.path
}

func (f *localFile) GetName() string {
	return f.name
}

func (f *localFile) GetSize() int64 {
	return f.info.Size()
}

func (f *localFile) GetTime() int64 {
	return f.info.ModTime().Unix()
}

func (f *localFile) GetMeta() []byte {
	return f.metaEnc
}

type meta struct {
	Link string `json:"link"`
}

func encodeMeta(m *meta) ([]byte, error) {
	return json.Marshal(m)
}

func decodeMeta(encoded []byte) (*meta, error) {
	m := meta{}
	err := json.Unmarshal(encoded, &m)
	return &m, err
}

// Local implements sts.FileStore for local files
type Local struct {
	Root           string
	MinAge         time.Duration
	IncludeHidden  bool
	Include        []*regexp.Regexp
	Ignore         []*regexp.Regexp
	FollowSymlinks bool
	scanTimeStart  time.Time
	scanTimeEnd    time.Time
	scanFiles      []sts.File
	cached         func(string) sts.File
}

// AddStandardIgnore adds standard ignore patterns
func (dir *Local) AddStandardIgnore() {
	ignore := []*regexp.Regexp{
		regexp.MustCompile(
			fmt.Sprintf(
				`(?:^|%s)%s$`,
				regexp.QuoteMeta(string(filepath.Separator)),
				regexp.QuoteMeta(fileutil.LockExt))),
		regexp.MustCompile(
			fmt.Sprintf(
				`(?:^|%s)%s$`,
				regexp.QuoteMeta(string(filepath.Separator)),
				regexp.QuoteMeta(disabledName))),
	}
	dir.Ignore = append(dir.Ignore, ignore...)
}

// Scan reads the root directory tree and builds a list of files to be returned.
// A non-nil error will result in no files being returned.
func (dir *Local) Scan(cached func(string) sts.File) ([]sts.File, time.Time, error) {
	var err error
	if _, err = os.Lstat(filepath.Join(dir.Root, disabledName)); err == nil {
		return nil, time.Time{}, nil
	}
	dir.scanTimeStart = time.Now()
	defer func() {
		dir.scanTimeEnd = time.Now()
	}()
	dir.scanFiles = nil
	dir.cached = cached
	dir.debug("Scanning Directory:", dir.Root)
	if err = fileutil.Walk(dir.Root, dir.handleNode, dir.FollowSymlinks); err != nil {
		return nil, dir.scanTimeStart, err
	}
	return dir.scanFiles,
		// Subtract the minimum age from the scan time
		dir.scanTimeStart.Add(-1 * dir.MinAge),
		nil
}

func (dir *Local) getRelPath(path string) string {
	return strings.Replace(path, dir.Root+string(os.PathSeparator), "", 1)
}

func (dir *Local) shouldIgnore(relPath string, isDir bool) bool {
	if !dir.IncludeHidden && strings.HasPrefix(filepath.Base(relPath), ".") {
		return true
	}
	var pattern *regexp.Regexp
	for _, pattern = range dir.Ignore {
		if pattern.MatchString(relPath) {
			return true
		}
	}
	if !isDir && len(dir.Include) > 0 {
		for _, pattern = range dir.Include {
			if pattern.MatchString(relPath) {
				return false
			}
		}
		return true
	}
	return false
}

func (dir *Local) handleNode(path string, info os.FileInfo, err error) error {
	if info == nil || err != nil {
		// If a file just doesn't exist anymore, we probably don't need to stop
		// the scan for that.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	relPath := dir.getRelPath(path)
	if info.IsDir() {
		if dir.shouldIgnore(relPath, true) {
			dir.debug("Ignored Local Directory:", path)
			// Best to skip entire directories if they match an ignore pattern.
			return filepath.SkipDir
		}
		dir.debug("Scanning Subdirectory:", path)
		return nil
	}
	if dir.shouldIgnore(relPath, false) {
		dir.debug("Ignored Local File:", path)
		return nil
	}
	fTime := info.ModTime()
	// It's important we use the same time reference (i.e. not "now") for
	// determining age to make sure we don't exclude some files from a scan but
	// not others that shoud have gone later.  This can happen because the
	// order in which nodes are handled is nondeterministic.
	fAge := dir.scanTimeStart.Sub(fTime)
	if fAge < dir.MinAge {
		return nil
	}
	if dir.cached != nil {
		cached := dir.cached(relPath)
		if cached != nil {
			if cached.GetTime() == fTime.Unix() &&
				cached.GetSize() == info.Size() {
				return nil
			}
		}
	}
	var file *localFile
	if file, err = newLocalFile(path, relPath, info); err != nil {
		if err == filepath.SkipDir {
			return nil
		}
		return err
	}
	dir.debug("Found Local File:", path)
	dir.scanFiles = append(dir.scanFiles, file)
	return nil
}

// Remove deletes the local file from disk
func (dir *Local) Remove(file sts.File) (err error) {
	return os.Remove(file.GetPath())
}

// Sync checks to see if a file on disk has changed
func (dir *Local) Sync(origFile sts.File) (newFile sts.File, err error) {
	var info os.FileInfo
	var file *localFile
	if info, err = os.Lstat(origFile.GetPath()); err != nil {
		return
	}
	if file, err = newLocalFile(origFile.GetPath(), origFile.GetName(), info); err != nil {
		return
	}
	newFile = file
	if file.GetTime() != origFile.GetTime() {
		return
	}
	if file.GetSize() != origFile.GetSize() {
		return
	}
	if string(file.GetMeta()) != string(origFile.GetMeta()) {
		return
	}
	newFile = nil
	return
}

// IsNotExist returns whether or not an error returned by Sync or Open
// indicates the file in question does not exist
func (dir *Local) IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

// GetOpener returns an Open function for reading
func (dir *Local) GetOpener() sts.Open {
	return dir.Open
}

// Open returns a readable for a local file instance
func (dir *Local) Open(origFile sts.File) (sts.Readable, error) {
	path := origFile.GetPath()
	metaEnc := origFile.GetMeta()
	if len(metaEnc) > 0 {
		meta, err := decodeMeta(metaEnc)
		if err != nil {
			return nil, err
		}
		if meta.Link != "" {
			path = meta.Link
		}
	}
	return os.Open(path)
}

// debug logs a debug message if and only if this is the first scan
func (dir *Local) debug(params ...interface{}) {
	if dir.scanTimeEnd.IsZero() {
		log.Debug(append([]interface{}{"File Store =>"}, params)...)
	}
}
