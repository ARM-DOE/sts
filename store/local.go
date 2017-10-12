package store

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
)

// disabledName is the file name used to disable scanning externally.
const disabledName = ".disabled"

type localFile struct {
	path    string
	relPath string
	info    os.FileInfo
}

func newLocalFile(path string, relPath string, info os.FileInfo) (f *localFile, err error) {
	f = &localFile{
		path:    path,
		relPath: relPath,
		info:    info,
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
			err = fmt.Errorf("Found symbolic link to directory: %s", path)
			return
		}
	}
	return
}

func (f *localFile) GetPath() string {
	return f.path
}

func (f *localFile) GetRelPath() string {
	return f.relPath
}

func (f *localFile) GetSize() int64 {
	return f.info.Size()
}

func (f *localFile) GetTime() int64 {
	return f.info.ModTime().Unix()
}

// Local implements sts.FileStore for local files
type Local struct {
	Root           string
	MinAge         time.Duration
	Include        []*regexp.Regexp
	Ignore         []*regexp.Regexp
	FollowSymlinks bool
	scanTime       time.Time
	scanFiles      []sts.File
	cached         func(string) sts.File
}

// Scan reads the root directory tree and builds a list of files to be returned.
// A non-nil error will result in no files being returned.
func (dir *Local) Scan(cached func(string) sts.File) ([]sts.File, time.Time, error) {
	var err error
	if _, err = os.Lstat(filepath.Join(dir.Root, disabledName)); err == nil {
		return nil, time.Time{}, nil
	}
	dir.scanTime = time.Now()
	dir.scanFiles = nil
	dir.cached = cached
	if err = fileutil.Walk(dir.Root, dir.handleNode, dir.FollowSymlinks); err != nil {
		return nil, dir.scanTime, err
	}
	return dir.scanFiles,
		// Subtract the minimum age from the scan time
		dir.scanTime.Add(-1 * dir.MinAge),
		nil
}

func (dir *Local) getRelPath(path string) string {
	return strings.Replace(path, dir.Root+string(os.PathSeparator), "", 1)
}

func (dir *Local) shouldIgnore(relPath string, isDir bool) bool {
	var pattern *regexp.Regexp
	if !isDir && len(dir.Include) > 0 {
		for _, pattern = range dir.Include {
			if pattern.MatchString(relPath) {
				return false
			}
		}
		return true
	}
	for _, pattern = range dir.Ignore {
		if pattern.MatchString(relPath) {
			return true
		}
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
			// Best to skip entire directories if they match an ignore pattern.
			return filepath.SkipDir
		}
		return nil
	}
	if dir.shouldIgnore(relPath, false) {
		return nil
	}
	fTime := info.ModTime()
	fAge := time.Now().Sub(fTime)
	if fAge < dir.MinAge {
		return nil
	}
	cached := dir.cached(relPath)
	if cached != nil {
		if cached.GetTime() == fTime.Unix() && cached.GetSize() == info.Size() {
			return nil
		}
	}
	var file *localFile
	if file, err = newLocalFile(path, relPath, info); err != nil {
		return err
	}
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
	if file, err = newLocalFile(origFile.GetPath(), origFile.GetRelPath(), info); err != nil {
		return
	}
	newFile = file
	if file.GetTime() != origFile.GetTime() {
		return
	}
	if file.GetSize() != origFile.GetSize() {
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
	return os.Open(origFile.GetPath())
}
