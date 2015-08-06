package util

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "time"
)

// Listener is a data struct that scans the filesystem for any additions to a directory.
// When an addition is detected, it is passed down the update_channel.
// Listener maintains a local file that contains a timestamp for when it last checked a directory.
// This local cache also has the capacity to store information about files.
type Listener struct {
    file_name      string
    last_update    int64
    Files          map[string]int64
    watch_dir      string // This is the directory that the cache holds data about
    decoder        gob.Decoder
    update_channel chan string
}

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
    _, err := os.Open(listener.file_name)
    if os.IsNotExist(err) {
        // Cache file does not exist, create new one
        os.Create(listener.file_name)
        listener.last_update = GetTimestamp()
        listener.WriteCache()
    }
    raw_file_bytes, _ := ioutil.ReadFile(listener.file_name)
    byte_buffer := bytes.NewBuffer(raw_file_bytes)
    decoder := gob.NewDecoder(byte_buffer)
    var new_map map[string]int64
    decode_err := decoder.Decode(&new_map)
    if decode_err != nil {
        listener.codingError(decode_err)
    }
    listener.Files = new_map
    listener.last_update = listener.Files["__TIMESTAMP__"]
}

// WriteCache dumps the in-memory cache to local file.
func (listener *Listener) WriteCache() {
    listener.Files["__TIMESTAMP__"] = listener.last_update
    byte_buffer := new(bytes.Buffer)
    cache_encoder := gob.NewEncoder(byte_buffer)
    encode_err := cache_encoder.Encode(listener.Files)
    if encode_err != nil {
        listener.codingError(encode_err)
    }
    // Create temporary cache file while writing.
    ioutil.WriteFile(listener.file_name+".tmp", byte_buffer.Bytes(), 0644)
    os.Rename(listener.file_name+".tmp", listener.file_name)
}

// GetTimestamp returns an int64 containing the current time since the epoch in seconds.
func GetTimestamp() int64 {
    now_time := time.Now()
    return now_time.Unix()
}

// scanDir recursively checks a directory for files that are newer than the current timestamp.
// If new files are found, their paths are passed up update_channel.
// When finished, it updates the local cache file.
func (listener *Listener) scanDir() {
    filepath.Walk(listener.watch_dir, listener.fileWalkHandler)
    listener.last_update = GetTimestamp()
    listener.WriteCache()
}

// fileWalkHandler is called for every file and directory in the directory managed by the Listener instance.
// It checks the managed directory for any files that have been modified since the last cache update, and sends them up update_channel.
func (listener *Listener) fileWalkHandler(path string, info os.FileInfo, err error) error {
    if !info.IsDir() {
        _, in_map := listener.Files[path]
        modtime := info.ModTime()
        if modtime == time.Unix(listener.last_update, 0) { // Special case: since ModTime doesn't offer resolution to a fraction of a second
            modtime = time.Unix(listener.last_update-1, 0) // If modtime and the last cache update are equal, shift ModTime back by one second.
        }
        if !in_map && modtime.After(time.Unix(listener.last_update, 0)) && info.Name() != ".DS_Store" {
            listener.update_channel <- path
        }
    }
    return nil
}

// Listen is a loop that operates on the watched directory, adding new files.
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
