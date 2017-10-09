package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
)

type cacheFile struct {
	path    string
	relPath string
	Size    int64  `json:"size"`
	Time    int64  `json:"mtime"`
	Hash    string `json:"hash"`
	Done    bool   `json:"done"`
}

func (f *cacheFile) GetPath() string {
	return f.path
}

func (f *cacheFile) GetRelPath() string {
	return f.relPath
}

func (f *cacheFile) GetSize() int64 {
	return f.Size
}

func (f *cacheFile) GetTime() int64 {
	return f.Time
}

func (f *cacheFile) GetHash() string {
	return f.Hash
}

func (f *cacheFile) IsDone() bool {
	return f.Done
}

// JSON implements sts.FileCache for making a local cache of file info
type JSON struct {
	Dir   string                `json:"dir"`
	Files map[string]*cacheFile `json:"files"`
	mutex sync.RWMutex
	path  string
	dirty bool
}

// NewJSON initializes the cache
func NewJSON(dir, key string) (j *JSON, err error) {
	if key == "" {
		key = dir
	}
	name := fmt.Sprintf("%s.json", fileutil.StringMD5(key))
	j = &JSON{
		Dir:   dir,
		Files: make(map[string]*cacheFile),
		path:  filepath.Join(dir, name),
	}
	if err = fileutil.LoadJSON(j.path, j); err != nil && !os.IsNotExist(err) {
		return
	}
	err = nil
	for k, f := range j.Files {
		f.path = filepath.Join(j.Dir, k)
		f.relPath = k
	}
	return
}

// Iterate iterates over the cache and calls provided callback on each file
func (j *JSON) Iterate(f func(sts.Cached) bool) {
	j.mutex.RLock()
	files := make([]*cacheFile, len(j.Files))
	i := 0
	for _, file := range j.Files {
		files[i] = file
		i++
	}
	j.mutex.RUnlock()
	for _, file := range files {
		if f(file) {
			break
		}
	}

}

// Get returns the stored file with the specified key
func (j *JSON) Get(key string) sts.Cached {
	j.mutex.RLock()
	defer j.mutex.RUnlock()
	if f, ok := j.Files[key]; ok {
		return f
	}
	return nil
}

// Add adds a hashed file to the in-memory cache
func (j *JSON) Add(file sts.Hashed) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.add(file)
}

// Done marks the file by the given key as done
func (j *JSON) Done(key string) {
	f := j.Get(key)
	if f == nil {
		return
	}
	j.mutex.Lock()
	defer j.mutex.Unlock()
	if f.IsDone() {
		return
	}
	j.dirty = true
	f.(*cacheFile).Done = true
}

// Remove removes the file by the specified key from the in-memory cache
func (j *JSON) Remove(key string) {
	f := j.Get(key)
	if f == nil {
		return
	}
	j.mutex.Lock()
	defer j.mutex.Unlock()
	delete(j.Files, key)
	j.dirty = true
}

// Persist writes the in-memory cache to disk
func (j *JSON) Persist() (err error) {
	if !j.dirty {
		return
	}
	err = j.write()
	return
}

func (j *JSON) add(file sts.Hashed) {
	if existing, ok := j.Files[file.GetRelPath()]; ok {
		existing.Size = file.GetSize()
		existing.Time = file.GetTime()
		existing.Hash = file.GetHash()
		return
	}
	j.Files[file.GetRelPath()] = &cacheFile{
		path:    file.GetPath(),
		relPath: file.GetRelPath(),
		Size:    file.GetSize(),
		Time:    file.GetTime(),
		Hash:    file.GetHash(),
	}
	j.dirty = true
}

func (j *JSON) write() error {
	if err := fileutil.WriteJSON(j.path, j); err != nil {
		return err
	}
	j.dirty = false
	return nil
}
