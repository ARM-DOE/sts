package main

import (
    "crypto/md5"
    "fmt"
    "github.com/rjeczalik/notify"
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
    "strings"
)

// Listener is a data type that monitors changes on the file system, generates, and sends JSON metadata for any new files.
// Listener contains a 3rd party package. "notify", which is used to detect changes on the file system without polling.
type Listener struct {
    directory          string
    walked_directories []string
    event_channel      chan notify.EventInfo
    file_queue         chan string
    watch_directory    string
}

// ListenerFactory creates and returns a new Listener struct.
func ListenerFactory(watch_directory string) *Listener {
    directory_list := make([]string, 0, 10)
    file_queue := make(chan string, 1)
    event_channel := make(chan notify.EventInfo, 1)
    notify.Watch(watch_directory+"/...", event_channel, notify.Create)

    new_listener := &Listener{}
    new_listener.directory = watch_directory
    new_listener.walked_directories = directory_list
    new_listener.event_channel = event_channel
    new_listener.file_queue = file_queue
    new_listener.watch_directory = watch_directory
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
    md5_hash := md5.New()
    io.WriteString(md5_hash, string(bytes_of_file))
    fmt.Println(file_path, listener.watch_directory)
    store_path := strings.Replace(file_path, filepath.Dir(listener.watch_directory)+"/", "", 1)
    json_summary := fmt.Sprintf(`{"file": {"path": "%s", "md5": "%x", "size": "%d", "modtime": "%s", "store_path": "%s"}}`, file_path, md5_hash.Sum(nil), info.Size(), info.ModTime(), store_path)
    listener.file_queue <- json_summary
    fmt.Println(json_summary)
}

// listen is the mainloop of a listener object.
// It blocks until a file addition event is detected, after which it calls handleAddition to process the new file.
func (listener *Listener) listen() {
    defer notify.Stop(listener.event_channel)
    fmt.Println("Listening")
    for true {
        select {
        case event := <-listener.event_channel:
            if len(event.Path()) > 0 {
                go listener.handleAddition(event.Path())
            }
        }
    }
}
