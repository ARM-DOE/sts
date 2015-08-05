package main

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "time"
    "util"
)

// Cache is a data struct that contains and manages sending progress of all unsent files.
// Cache maintains a local file that has a record of the status of all unsent files.
// Cache also contains methods for scanning the file system for changes.
type Cache struct {
    file_name       string
    last_update     int64
    files           map[string]int64
    watch_dir       string // This is the directory that the cache holds data about
    decoder         gob.Decoder
    bin_channel     chan Bin
    files_available bool
}

// CacheFactory generates and returns a new Cache struct which operates on the provided cache_file_name, and contains data about the files in watch_dir.
func CacheFactory(cache_file_name string, watch_dir string, bin_channel chan Bin) *Cache {
    new_cache := &Cache{}
    new_cache.file_name = cache_file_name
    new_cache.last_update = -1
    new_cache.watch_dir = watch_dir
    new_cache.files = make(map[string]int64)
    new_cache.bin_channel = bin_channel
    return new_cache
}

// loadCache replaces the in-memory cache with the cache saved to disk.
func (cache *Cache) loadCache() {
    _, err := os.Open(cache.file_name)
    if os.IsNotExist(err) {
        // Cache file does not exist, create new one
        os.Create(cache.file_name)
        cache.last_update = getTimestamp()
        cache.writeCache()
    }
    raw_file_bytes, _ := ioutil.ReadFile(cache.file_name)
    byte_buffer := bytes.NewBuffer(raw_file_bytes)
    decoder := gob.NewDecoder(byte_buffer)
    var new_map map[string]int64
    decode_err := decoder.Decode(&new_map)
    if decode_err != nil {
        cache.codingError(decode_err)
    }
    cache.files = new_map
    cache.last_update = cache.files["__TIMESTAMP__"]
}

// writeCache dumps the in-memory cache to local file.
func (cache *Cache) writeCache() {
    cache.files["__TIMESTAMP__"] = cache.last_update
    byte_buffer := new(bytes.Buffer)
    cache_encoder := gob.NewEncoder(byte_buffer)
    encode_err := cache_encoder.Encode(cache.files)
    if encode_err != nil {
        cache.codingError(encode_err)
    }
    // Create temporary cache file while writing.
    ioutil.WriteFile(cache.file_name+".tmp", byte_buffer.Bytes(), 0644)
    os.Rename(cache.file_name+".tmp", cache.file_name)
}

// getTimestamp returns an int64 containing the current time since the epoch in seconds.
func getTimestamp() int64 {
    now_time := time.Now()
    return now_time.Unix()
}

// addFile adds a new file to the cache, and updates the local copy of the cache.
func (cache *Cache) addFile(path string) {
    cache.files[path] = 0
    cache.writeCache()
}

// updateFile updates the record of bytes that have been allocated to a Bin.
// Take note that updateFile does NOT update the local cache.
func (cache *Cache) updateFile(path string, new_byte_progress int64, info os.FileInfo) {
    if new_byte_progress == info.Size() {
        new_byte_progress = -1
    }
    if new_byte_progress > info.Size() {
        panic("Bin stored too many bytes")
    }
    cache.files[path] = new_byte_progress
}

// removeFile deletes a file and it's progress from the cache.
// It should only be used when a file has been confirmed to have completely sent.
func (cache *Cache) removeFile(path string) {
    delete(cache.files, path)
    cache.writeCache()
}

// scanDir recursively checks a directory for files that aren't in the cache, and adds them.
// If new files are found, allocate() is called, and Bins are created until all parts of the new files have been allocated to a Bin.
// When finished, it updates the local cache file.
func (cache *Cache) scanDir() {
    filepath.Walk(cache.watch_dir, cache.fileWalkHandler)
    cache.last_update = getTimestamp()
    if cache.files_available {
        cache.allocate()
    }
    cache.writeCache()
}

// allocate is called when a new file is detected.
// While there are still any unallocated bytes in the cache, allocate continuously fills new Bins.
// When a new non-empty Bin is filled, it is sent down the Bin channel.
// Allocate stops when a Bin is filled, but is empty.
func (cache *Cache) allocate() {
    for cache.files_available {
        new_bin := BinFactory(util.BIN_SIZE, cache.watch_dir)
        new_bin.fill(cache)
        if new_bin.Empty {
            cache.files_available = false
        } else {
            cache.bin_channel <- new_bin
        }
    }
}

// fileWalkHandler is called for every file and directory in the directory managed by the Cache instance.
// It checks the managed directory for any files that have been modified since the last cache update, and adds them to the in-memory cache.
func (cache *Cache) fileWalkHandler(path string, info os.FileInfo, err error) error {
    if !info.IsDir() {
        _, in_map := cache.files[path]
        modtime := info.ModTime()
        if modtime == time.Unix(cache.last_update, 0) { // Special case: since ModTime doesn't offer resolution to a fraction of a second
            modtime = time.Unix(cache.last_update-1, 0) // If modtime and the last cache update are equal, shift ModTime back by one second.
        }
        if !in_map && modtime.After(time.Unix(cache.last_update, 0)) && info.Name() != ".DS_Store" {
            cache.files[path] = 0
            cache.files_available = true
        }
    }
    return nil
}

// listen is a loop that operates on the cache, adding new files.
// It scans the the specified watch directory every few seconds for any file additions.
// Due to its need to continuously scan the file system, it should always be called by a goroutine
func (cache *Cache) listen() {
    for {
        cache.scanDir()
        time.Sleep(3 * time.Second)
    }
}

// codingError is called when there is an error encoding the in-memory cache, or decoding the local copy of the cache.
// This would probably only be called if the local copy of the cache has been corrupted.
// Because of this, codingError tries to provide a meaningful dump of data in regards to the status of the cache.
func (cache *Cache) codingError(err error) {
    fmt.Println("Cache corruption in " + cache.file_name)
    fmt.Println("Cache contents dump: ", cache.files)
    panic(err)
}
