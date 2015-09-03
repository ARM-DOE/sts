package util

import (
    "encoding/json"
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
    file_name       string           // Name of the cache file that the listener uses to store timestamp and file data
    last_update     int64            // Time of last update in seconds since the epoch
    Files           map[string]int64 // Storage for timestamp and any additional file data. This data structure is written to the local cache.
    watch_dirs      []string         // This is the directory that the cache holds data about
    update_channel  chan string      // The channel that new and valid file paths will be sent to upon detection
    onFinish        FinishFunc       // Function called when a scan that detects new files finishes.
    new_files       bool             // Set to true if there were new files found in the scan
    scan_delay      int              // Delay for how often the listener should scan the file system.
    ignore_patterns []string         // A list of patterns that each new file name (NOT path) is tested against. If a match is found the file is ignored.
    recent_files    []string         // A list of files that were found in the most recent string of updates.
    write_lock      sync.Mutex       // Prevents the cache from being written to or modified simultaneously
    error_log       Logger
}

// FinishFunc is the function called after a scan that detects new files
// It can be used to address the whole group of files found during scan,
// rather than processing them one at a time.
type FinishFunc func()

// NewListener generates and returns a new Listener struct which operates on the provided
// watch_dir and saves its data to cache_file_name.
func NewListener(cache_file_name string, error_log Logger, watch_dirs ...string) *Listener {
    new_listener := &Listener{}
    new_listener.error_log = error_log
    new_listener.recent_files = make([]string, 0)
    new_listener.scan_delay = 3
    new_listener.file_name = cache_file_name
    new_listener.last_update = -1
    new_listener.watch_dirs = watch_dirs
    new_listener.Files = make(map[string]int64)
    new_listener.ignore_patterns = make([]string, 0)
    new_listener.AddIgnored(`\.DS_Store`)
    return new_listener
}

// LoadCache replaces the in-memory cache with the cache saved to disk.
func (listener *Listener) LoadCache() {
    _, err := os.Stat(listener.file_name)
    if os.IsNotExist(err) {
        // Cache file does not exist, create new one
        os.Create(listener.file_name)
        listener.last_update = GetTimestamp()
        listener.WriteCache()
    }
    fi, _ := os.Open(listener.file_name)
    new_map := make(map[string]int64)
    json_decoder := json.NewDecoder(fi)
    decode_err := json_decoder.Decode(&new_map)
    if decode_err != nil && decode_err != io.EOF {
        listener.codingError(decode_err)
    }
    listener.Files = new_map
    listener.last_update = listener.Files["__TIMESTAMP__"]
}

// WriteCache dumps the in-memory cache to local file.
func (listener *Listener) WriteCache() {
    listener.write_lock.Lock()
    defer listener.write_lock.Unlock()
    listener.Files["__TIMESTAMP__"] = listener.last_update
    json_bytes, encode_err := json.Marshal(listener.Files)
    if encode_err != nil {
        listener.codingError(encode_err)
    }
    // Create temporary cache file while writing.
    ioutil.WriteFile(listener.file_name+".tmp", json_bytes, 0644)
    os.Rename(listener.file_name+".tmp", listener.file_name)
}

// scanDir recursively checks a directory for files that are newer than the current timestamp.
// If new files are found, their paths are sent to update_channel.
// When finished, it updates the local cache file.
func (listener *Listener) scanDir() {
    for _, watch_dir := range listener.watch_dirs {
        if (!listener.new_files || len(listener.recent_files) > 1000) && len(listener.recent_files) > 0 {
            // Clear recent files if files stop coming or if the recent_files cache is huge.
            listener.recent_files = make([]string, 0)
        }
        listener.new_files = false
        filepath.Walk(watch_dir, listener.fileWalkHandler)
    }
    listener.last_update = GetTimestamp()
    listener.afterScan()
    listener.WriteCache()
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
        in_recent := IsStringInArray(listener.recent_files, path)
        modtime := info.ModTime()
        special_case := false
        if modtime == time.Unix(listener.last_update, 0) && !in_map {
            // Special case: since ModTime doesn't offer resolution to a fraction of a second
            special_case = true // If modtime and the last cache update are equal, set the special case flag
            // When the special case flag is active, the modtime check in the next conditional always passes.
        }
        if !in_recent && !in_map && !listener.checkIgnored(info.Name()) && (modtime.After(time.Unix(listener.last_update, 0)) || special_case) {
            listener.new_files = true
            listener.recent_files = append(listener.recent_files, path)
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
            listener.error_log.LogError("Error matching pattern", pattern, "against file", path)
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

func (listener *Listener) WatchDir(dir_slice int) string {
    return listener.watch_dirs[dir_slice]
}

// codingError is called when there is an error encoding the in-memory cache, or decoding the local copy of the cache.
// This would probably only be called if the local copy of the cache has been corrupted.
// Because of this, codingError tries to provide a meaningful dump of data in regards to the status of the cache.
func (listener *Listener) codingError(err error) {
    listener.error_log.LogError("Cache corruption in " + listener.file_name)
    listener.error_log.LogError("Cache contents dump:", listener.Files)
    listener.error_log.LogError(err)
    panic("")
}
