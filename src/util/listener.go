package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

// Listener is a data struct that scans the file system for any additions to a directory.
// When an addition is detected, it is passed to update_channel.
// Listener maintains a local file that contains a timestamp for when it last checked a directory.
// This local cache also has the capacity to store information about files.
type Listener struct {
	file_name            string               // Name of the cache file that the listener uses to store timestamp and file data
	last_update          int64                // Time of last update in seconds since the epoch
	Files                map[string]CacheFile // Storage for timestamp and any additional file data. This data structure is written to the local cache.
	watch_dirs           []string             // This is the directory that the cache holds data about
	update_channel       chan string          // The channel that new and valid file paths will be sent to upon detection
	onFinish             FinishFunc           // Function called when a scan that detects new files finishes.
	new_files            bool                 // Set to true if there were new files found in the scan
	scan_delay           int                  // Delay for how often the listener should scan the file system.
	ignore_patterns      []string             // A list of patterns that each new file name (NOT path) is tested against. If a match is found the file is ignored.
	recent_files         map[string]bool      // A list of files that were found in the most recent string of updates. Stored as a map for quick lookups.
	WriteLock            sync.Mutex           // Prevents the cache from being written to disk and modified simultaneously
	cache_write_interval int64                // Time in seconds that must elapse before the on-disk cache file can be written again.
	last_cache_write     time.Time            // The time that the cache was last written to disk.
}

// CacheFile is the data structure that is stored on disk and in memory for every file in the cache
// Only public fields are saved to disk.
type CacheFile struct {
	Allocation int64    // An int64 that represents the number of bytes from this file that have been allocated to a Bin.
	StartTime  int64    // A unix timestamp that is taken when the first part of a file is allocated. Used to calculate time taken for send.
	tag        *TagData // Value used to cache tag lookups. Isn't written to disk.
	Bytes      int64    // The file size when cached.
	SentBytes  int64    // The number of bytes sent (updated as bins for this file are deleted).
}

func (file *CacheFile) SetAllocation(new_allocation int64) {
	file.Allocation = new_allocation
}

func (file *CacheFile) SetTag(tag *TagData) {
	file.tag = tag
}

// HasTag returns a tag and true if a TagData has already been looked up and cached for the file,
// if a file hasn't be cached, it will return false and nil.
func (file *CacheFile) HasTag() (*TagData, bool) {
	if file.tag != nil {
		return file.tag, true
	}
	return nil, false
}

// FinishFunc is the function called after a scan that detects new files
// It can be used to address the whole group of files found during scan,
// rather than processing them one at a time.
type FinishFunc func()

// NewListener generates and returns a new Listener struct which operates on the provided
// watch_dir and saves its data to cache_file_name.
func NewListener(cache_file_name string, cache_write_interval int64, watch_dirs ...string) *Listener {
	new_listener := &Listener{}
	new_listener.recent_files = make(map[string]bool)
	new_listener.scan_delay = 3
	new_listener.file_name = cache_file_name
	new_listener.cache_write_interval = cache_write_interval
	new_listener.last_cache_write = time.Now()
	new_listener.last_update = -1
	new_listener.watch_dirs = watch_dirs
	new_listener.Files = make(map[string]CacheFile)
	new_listener.ignore_patterns = make([]string, 0)
	new_listener.AddIgnored(`\.DS_Store`)
	return new_listener
}

// LoadCache replaces the in-memory cache with the cache saved to disk.
func (listener *Listener) LoadCache() error {
	_, err := os.Stat(listener.file_name)
	if os.IsNotExist(err) {
		// Cache file does not exist, create new one
		os.Create(listener.file_name)
		listener.last_update = 0
		listener.WriteCache()
	}
	fi, open_err := os.Open(listener.file_name)
	if open_err != nil {
		return open_err
	}
	new_map := make(map[string]CacheFile)
	json_decoder := json.NewDecoder(fi)
	decode_err := json_decoder.Decode(&new_map)
	if decode_err != nil && decode_err != io.EOF {
		listener.codingError(decode_err)
	}
	listener.WriteLock.Lock()
	defer listener.WriteLock.Unlock()
	listener.Files = new_map
	listener.last_update = listener.Files["__TIMESTAMP__"].Allocation
	return nil
}

// WriteCache dumps the in-memory cache to local file.
func (listener *Listener) WriteCache() {
	listener.WriteLock.Lock()
	defer listener.WriteLock.Unlock()
	// Only write to the cache if it hasn't been written for cache_write_interval seconds.
	if time.Since(listener.last_cache_write).Seconds() > float64(listener.cache_write_interval) {
		listener.Files["__TIMESTAMP__"] = CacheFile{listener.last_update, 0, nil, 0, 0}
		json_bytes, encode_err := json.Marshal(listener.Files)
		if encode_err != nil {
			listener.codingError(encode_err)
		}
		// Create temporary cache file while writing.
		ioutil.WriteFile(listener.file_name+".tmp", json_bytes, 0644)
		os.Rename(listener.file_name+".tmp", listener.file_name)
		// Update last cache timestamp
		listener.last_cache_write = time.Now()
	}
}

// scanDir recursively checks a directory for files that are newer than the current timestamp.
// If new files are found, their paths are sent to update_channel.
// When finished, it updates the local cache file.
func (listener *Listener) scanDir() {
	before_update := GetTimestamp()
	listener.new_files = false
	for _, watch_dir := range listener.watch_dirs {
		filepath.Walk(watch_dir, listener.fileWalkHandler)
	}
	listener.last_update = before_update
	listener.afterScan()
	listener.WriteCache()
	if !listener.new_files && len(listener.recent_files) > 0 {
		// Clear recent files if files stop coming for one update.
		listener.recent_files = make(map[string]bool)
	}
}

// fileWalkHandler is called for every file and directory in the directory managed by the Listener instance.
// It checks the managed directory for any files that have been modified since the last cache update
// and sends them to update_channel.
func (listener *Listener) fileWalkHandler(path string, info os.FileInfo, err error) error {
	if info == nil { // Check to make sure the file still exists
		return nil
	}
	if !info.IsDir() {
		_, in_map := listener.Files[path]
		_, in_recent := listener.recent_files[path]
		modtime := info.ModTime()
		special_case := false
		if modtime == time.Unix(listener.last_update, 0) && !in_map {
			// Special case: since ModTime doesn't offer resolution to a fraction of a second
			special_case = true // If modtime and the last cache update are equal, set the special case flag
			// When the special case flag is active, the modtime check in the next conditional always passes.
		}
		if !in_recent && !in_map && !listener.checkIgnored(info.Name()) && (modtime.After(time.Unix(listener.last_update, 0)) || special_case) {
			listener.new_files = true
			listener.recent_files[path] = true
			listener.update_channel <- path
		}
	}
	return nil
}

// afterScan is called by the listener after a scan completes
// If new files were detected, the settable onFinish function is called.
func (listener *Listener) afterScan() {
	if listener.onFinish != nil && listener.new_files {
		listener.onFinish()
	}
}

func (listener *Listener) SetOnFinish(onFinish FinishFunc) {
	listener.onFinish = onFinish
}

// checkIgnored is called every time a file is about to be sent to the addition channel.
// The file name is checked against every registered ignore pattern, if a match is found, the file is ignored.
func (listener *Listener) checkIgnored(path string) bool {
	ignore := false
	for _, pattern := range listener.ignore_patterns {
		matched, match_err := regexp.MatchString(pattern, path)
		if match_err != nil {
			LogError("Error matching pattern", pattern, "against file", path)
		} else {
			if matched {
				ignore = true
			}
		}
	}
	return ignore
}

// AddIgnored can be called to add a regex pattern to the list of ignored files.
func (listener *Listener) AddIgnored(pattern string) {
	listener.ignore_patterns = append(listener.ignore_patterns, pattern)
}

// Listen is a loop that operates on the watched directory, adding new files to update_channel.
// It scans the the specified watch directory every few seconds for any file additions.
// Make sure to call LoadCache before you call Listen, or the last timestamp won't be loaded from file.
// Due to its need to continuously scan the file system, it should always be called by a goroutine
func (listener *Listener) Listen(new_file chan string) {
	listener.update_channel = new_file
	for {
		listener.scanDir()
		time.Sleep(time.Duration(listener.scan_delay) * time.Second)
	}
}

func (listener *Listener) WatchDir(dir_slice int) (string, error) {
	if len(listener.watch_dirs)-1 < dir_slice {
		return "", errors.New(fmt.Sprintf("No watch dir registered with index %d", dir_slice))
	}
	return listener.watch_dirs[dir_slice], nil
}

// codingError is called when there is an error encoding the in-memory cache, or decoding the local copy of the cache.
// This would probably only be called if the local copy of the cache has been corrupted.
// Because of this, codingError tries to provide a meaningful dump of data in regards to the status of the cache.
func (listener *Listener) codingError(err error) {
	LogError("Cache corruption:", listener.file_name)
	LogError("Cache contents dump:", listener.Files)
	LogError(err)
	panic("Fatal error")
}
