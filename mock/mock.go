package mock

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/arm-doe/sts"
)

// File is meant to implement as many of the file-typed interfaces as possible
type File struct {
	Name     string
	Renamed  string
	Path     string
	Size     int64
	Time     time.Time
	Meta     []byte
	Hash     string
	SendTime int64
	Done     bool
	data     []byte
}

// GetPath gets the path
func (f *File) GetPath() string {
	return f.Path
}

// GetName gets the name part of the path
func (f *File) GetName() string {
	return f.Name
}

// GetRenamed gets the new name, if applicable (empty otherwise)
func (f *File) GetRenamed() string {
	return f.Renamed
}

// GetSize gets the file size
func (f *File) GetSize() int64 {
	return f.Size
}

// GetTime gets the file mod time
func (f *File) GetTime() time.Time {
	return f.Time
}

// GetMeta gets any additional metadata needed by the store implementation
func (f *File) GetMeta() []byte {
	return f.Meta
}

// GetHash gets the file signature
func (f *File) GetHash() string {
	return f.Hash
}

// TimeMs returns the time (in milliseconds) a file took to be sent
func (f *File) TimeMs() int64 {
	return f.SendTime
}

// IsDone is obvious
func (f *File) IsDone() bool {
	return f.Done
}

// RecoveredFile is meant to be a recovered and fully allocated file
type RecoveredFile struct {
	sts.Hashed
	Prev string
}

// GetPrev returns any configured previous file
func (f *RecoveredFile) GetPrev() string {
	return f.Prev
}

// Allocate does nothing
func (f *RecoveredFile) Allocate(int64) (int64, int64) {
	return 0, 0
}

// IsAllocated is always true
func (f *RecoveredFile) IsAllocated() bool {
	return true
}

// Readable mocks sts.Readable using a generic bytes reader
type Readable struct {
	*bytes.Reader
}

// Close is a noop just so we can satisfy the interface
func (Readable) Close() error { return nil }

// MissingError is a simple wrapper around the standard error
type MissingError struct {
	error
}

// Store mocks sts.FileSource
type Store struct {
	files map[string]*File
	mtime time.Time
	lock  sync.RWMutex
}

// NewStore creates a new Store with n files gradually ranging to size
func NewStore(n, size int) *Store {
	store := &Store{
		files: make(map[string]*File),
	}
	now := time.Now()
	total := int64(0)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("group%d%d/name%d.ext", i%3, i%2, i)
		path := fmt.Sprintf("/root/dir/%s", name)
		size := int64(size * (i + 1) / n)
		f := &File{
			Path: path,
			Name: name,
			Size: size,
			Time: now.Add(time.Duration(i-n) * time.Second),
			data: make([]byte, size),
		}
		_, _ = rand.Read(f.data)
		store.files[name] = f
		total += size
	}
	println(fmt.Sprintf("Total store size: %.2f MB", float64(total)/1024/1024))
	store.mtime = now.Add(time.Duration(-n) * time.Second)
	return store
}

// Scan implements sts.FileSource.Scan()
func (s *Store) Scan(allow func(sts.File) bool) ([]sts.File, time.Time, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var found []sts.File
	for _, f := range s.files {
		if !allow(f) {
			continue
		}
		found = append(found, f)
	}
	return found, s.mtime, nil
}

// Remove implements sts.FileSource.Remove()
func (s *Store) Remove(file sts.File) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.files, file.GetName())
	return nil
}

// Sync implements sts.FileSource.Sync()
func (s *Store) Sync(file sts.File) (sts.File, error) {
	return file, nil
}

// GetOpener implements sts.FileSource.GetOpener()
func (s *Store) GetOpener() sts.Open {
	return s.opener
}

// IsNotExist returns whether or not the provided error indicates a file is
// missing
func (s *Store) IsNotExist(err error) bool {
	_, ok := err.(MissingError)
	return ok
}

func (s *Store) ShouldIgnore(file sts.File) bool {
	// For testing purposes, ignore files with "ignore" in the name
	return bytes.Contains([]byte(file.GetName()), []byte("ignore"))
}

func (s *Store) opener(file sts.File) (sts.Readable, error) {
	if f, ok := file.(*File); ok {
		return Readable{bytes.NewReader(f.data)}, nil
	}
	if f, ok := s.files[file.GetName()]; ok {
		return Readable{bytes.NewReader(f.data)}, nil
	}
	return nil, MissingError{
		fmt.Errorf("unknown file: %s", file.GetPath()),
	}
}

// Cache implements sts.FileCache
type Cache struct {
	files map[string]*File
	mutex sync.RWMutex
	dirty bool
}

// NewCache initializes the cache
func NewCache() *Cache {
	return &Cache{
		files: make(map[string]*File),
	}
}

// Iterate iterates over the cache and calls provided callback on each file
func (c *Cache) Iterate(f func(sts.Cached) bool) {
	c.mutex.RLock()
	files := make([]*File, len(c.files))
	i := 0
	for _, file := range c.files {
		files[i] = file
		i++
	}
	c.mutex.RUnlock()
	for _, file := range files {
		if f(file) {
			break
		}
	}
}

// Get returns the stored file with the specified key
func (c *Cache) Get(key string) sts.Cached {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if f, ok := c.files[key]; ok {
		return f
	}
	return nil
}

// Add adds a hashed file to the in-memory cache
func (c *Cache) Add(file sts.Hashed) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.add(file)
}

// Done marks the file by the given key as done
func (c *Cache) Done(key string, whileLocked func(sts.Cached)) {
	f := c.Get(key)
	if f == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if f.IsDone() {
		return
	}
	c.dirty = true
	f.(*File).Done = true
	if whileLocked != nil {
		whileLocked(f)
	}
}

// Remove removes the file by the specified key from the in-memory cache
func (c *Cache) Remove(key string) {
	f := c.Get(key)
	if f == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.files, key)
	c.dirty = true
}

// Reset marks a file to be sent again
func (c *Cache) Reset(key string) {
	f := c.Get(key)
	if f == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	f.(*File).Hash = ""
}

// Persist writes the in-memory cache to disk
func (c *Cache) Persist() (err error) {
	err = c.write()
	return
}

func (c *Cache) add(file sts.Hashed) {
	c.dirty = true
	if existing, ok := c.files[file.GetName()]; ok {
		existing.Size = file.GetSize()
		existing.Time = file.GetTime()
		existing.Hash = file.GetHash()
		return
	}
	c.files[file.GetName()] = &File{
		Path: file.GetPath(),
		Name: file.GetName(),
		Size: file.GetSize(),
		Time: file.GetTime(),
		Meta: file.GetMeta(),
		Hash: file.GetHash(),
	}
}

func (c *Cache) write() error {
	c.dirty = false
	return nil
}

// Logger mocks logging by just printing to stdout/stderr
type Logger struct {
	DebugMode bool
	recent    []string
}

// Debug logs debug messages
func (log *Logger) Debug(params ...interface{}) {
	if log.DebugMode {
		fmt.Fprintln(os.Stdout, params...)
	}
}

// Info logs general information
func (log *Logger) Info(params ...interface{}) {
	log.recent = append(log.recent, fmt.Sprint(params...))
	fmt.Fprintln(os.Stdout, params...)
}

// Error logs ...errors
func (log *Logger) Error(params ...interface{}) {
	log.recent = append(log.recent, fmt.Sprint(params...))
	fmt.Fprintln(os.Stderr, params...)
}

// Recent returns recent messages (at least, it's supposed to)
func (log *Logger) Recent(n int) (msgs []string) {
	total := len(log.recent)
	offset := total - n
	if offset < 0 || n <= 0 {
		offset = 0
	}
	msgs = append(msgs, log.recent...)[offset:]
	return
}
