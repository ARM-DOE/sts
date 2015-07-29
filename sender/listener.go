package main

import (
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "strings"
    "time"
)

// Listener is a data type that monitors changes on the file system, generates, and sends JSON metadata for any new files.
type Listener struct {
    directory          string
    walked_directories []string
    addition_channel   chan string
    file_queue         chan string
    cache              *Cache
    watch_directory    string
}

// ListenerFactory creates and returns a new Listener struct.
func ListenerFactory(watch_directory string, cache *Cache) *Listener {
    directory_list := make([]string, 0, 10)
    file_queue := make(chan string, 1)
    new_listener := &Listener{}
    new_listener.directory = watch_directory
    new_listener.walked_directories = directory_list
    new_listener.file_queue = file_queue
    new_listener.watch_directory = watch_directory
    new_listener.cache = cache
    return new_listener
}

// handleAddition is called when a Listener struct detects a file addition.
// It generates JSON metadata for that file, and adds it to the queue.
// This function should be run asynchonously because it must calculate the md5 using the entire file, which may be a lengthy operation.
func (listener *Listener) handleAddition(file_path string) {
    // Manually build json string, probably a bad idea
    info, _ := os.Stat(file_path)
    bytes_of_file, err := ioutil.ReadFile(file_path)
    if err != nil {
        // Sad file go bye
        fmt.Println("Error while exporting ", file_path)
        return
    }
    store_path := strings.Replace(file_path, filepath.Dir(listener.watch_directory)+"/", "", 1)
    json_summary := fmt.Sprintf(`{"file": {"path": "%s", "md5": "%x", "size": "%d", "modtime": "%s", "store_path": "%s"}}`, file_path, generateMD5(bytes_of_file), info.Size(), info.ModTime(), store_path)
    listener.cache.addFile(file_path)
    listener.file_queue <- json_summary
    fmt.Println(json_summary)
}

// listen is one of the mainloops of a listener object.
// It scans the the specified watch directory every few seconds for any file additions
func (listener *Listener) listen() {
    for {
        listener.cache.scanDir()
        time.Sleep(3 * time.Second)
    }
}

// checkEvents is one of the mainloops of a listener object.
// It blocks until a file addition event is detected, after which it calls handleAddition to process the new file.
func (listener *Listener) checkEvents() {
    fmt.Println("Listening")
    for {
        select {
        case addition_event := <-listener.cache.addition_channel:
            go listener.handleAddition(addition_event)
        }
    }
}
