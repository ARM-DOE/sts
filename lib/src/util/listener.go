package util

import (
    "encoding/json"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
    "time"
)

// Listener is a data struct that scans the filesystem for any additions to a directory.
// When an addition is detected, it is passed to update_channel.
// Listener maintains a local file that contains a timestamp for when it last checked a directory.
// This local cache also has the capacity to store information about files.
type Listener struct {
    file_name      string
    last_update    int64
    Files          map[string]int64
    watch_dir      string // This is the directory that the cache holds data about
    update_channel chan string
    onFinish       FinishFunc // Function called when a scan that detects new files finishes.
    new_files      bool       // Set to true if there were new files found in the scan
}

// Function called after a scan that detects new files
type FinishFunc func()

// ListenerFactory generates and returns a new Listener struct which operates on the provided watch_dir, and saves it's data to cache_file_name.
func ListenerFactory(cache_file_name string, watch_dir string) *Listener {
    new_listener := &Listener{}
    new_listener.file_name = cache_file_name
    new_listener.last_update = -1
    new_listener.watch_dir = watch_dir
    new_listener.Files = make(map[string]int64)
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
    listener.Files["__TIMESTAMP__"] = listener.last_update
    json_bytes, encode_err := json.Marshal(listener.Files)
    if encode_err != nil {
        listener.codingError(encode_err)
    }
    // Create temporary cache file while writing.
    ioutil.WriteFile(listener.file_name+".tmp", json_bytes, 0644)
    os.Rename(listener.file_name+".tmp", listener.file_name)
}

// GetTimestamp returns an int64 containing the current time since the epoch in seconds.
func GetTimestamp() int64 {
    now_time := time.Now()
    return now_time.Unix()
}

// scanDir recursively checks a directory for files that are newer than the current timestamp.
// If new files are found, their paths are sent to update_channel.
// When finished, it updates the local cache file.
func (listener *Listener) scanDir() {
    listener.new_files = false
    filepath.Walk(listener.watch_dir, listener.fileWalkHandler)
    listener.last_update = GetTimestamp()
    listener.WriteCache()
    listener.afterScan()
}

// fileWalkHandler is called for every file and directory in the directory managed by the Listener instance.
// It checks the managed directory for any files that have been modified since the last cache update, and sends them to update_channel.
func (listener *Listener) fileWalkHandler(path string, info os.FileInfo, err error) error {
    if info == nil { // Check to make sure the file still exists
        return nil
    }
    if !info.IsDir() {
        _, in_map := listener.Files[path]
        modtime := info.ModTime()
        if modtime == time.Unix(listener.last_update, 0) { // Special case: since ModTime doesn't offer resolution to a fraction of a second
            modtime = time.Unix(listener.last_update-1, 0) // If modtime and the last cache update are equal, shift ModTime back by one second.
        }
        if !in_map && modtime.After(time.Unix(listener.last_update, 0)) && info.Name() != ".DS_Store" {
            listener.new_files = true
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

// Listen is a loop that operates on the watched directory, adding new files to update_channel.
// It scans the the specified watch directory every few seconds for any file additions.
// Make sure to call LoadCache before you call Listen, or the last timestamp won't be loaded from file.
// Due to its need to continuously scan the file system, it should always be called by a goroutine
func (listener *Listener) Listen(new_file chan string) {
    listener.update_channel = new_file
    for {
        listener.scanDir()
        time.Sleep(3 * time.Second)
    }
}

// codingError is called when there is an error encoding the in-memory cache, or decoding the local copy of the cache.
// This would probably only be called if the local copy of the cache has been corrupted.
// Because of this, codingError tries to provide a meaningful dump of data in regards to the status of the cache.
func (listener *Listener) codingError(err error) {
    fmt.Println("Cache corruption in " + listener.file_name)
    fmt.Println("Cache contents dump: ", listener.Files)
    panic(err)
}
