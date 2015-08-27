package main

import (
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "time"
    "util"
)

const TRANSFER_HTTP = "http"
const TRANSFER_DISK = "disk"
const TRANSFER_GRIDFTP = "gridftp"

var config util.Config

// main is the entry point for the sender program.
// It parses config values that are necessary during runtime,
// dispatches the listening and sending threads, and loops infinitely.
func main() {
    config = util.ParseConfig("config.yaml")
    checkWatchDir(config.Directory) // Exits if watch dir is not valid

    // Create the channel through which new bins will be sent from the sender to the receiver.
    bin_channel := make(chan Bin, config.Sender_Threads+5) // Create a bin channel with buffer size large enough to accommodate all sender threads and a little wiggle room.
    // Create and start cache file handler and webserver
    file_cache := NewCache(config.Cache_File_Name, config.Directory, config.Bin_Size, bin_channel)
    file_cache.listener.LoadCache()
    server := NewWebserver(file_cache)
    go server.startServer()

    // Dispatch senders
    senders := make([]*Sender, config.Sender_Threads)
    for dispatched_senders := 0; dispatched_senders < config.Sender_Threads; dispatched_senders++ {
        created_sender := NewSender(file_cache.bin_channel, config.Compression)
        go created_sender.run()
        senders[dispatched_senders] = created_sender
    }
    file_cache.SetSenders(senders)
    go file_cache.scan() // Start the file listener thread
    fmt.Println("Ready to send")
    for {
        time.Sleep(1 * time.Second)
    }
}

// getStorePath returns the path that the receiver should use to store a file.
// Given parameters full_path and watch_directory, it will remove watch directory from the full path.
func getStorePath(full_path string, watch_directory string) string {
    store_path := strings.Replace(full_path, filepath.Dir(watch_directory)+string(os.PathSeparator), "", 1)
    return store_path
}

// getWholePath returns the absolute path of the file given the path where the file will be stored on the receiver.
func getWholePath(store_path string) string {
    abs_path, err := filepath.Abs(store_path)
    if err != nil {
        fmt.Println(err.Error())
    }
    return abs_path
}

// checkWatchDir is called on the directory to be watched.
// It validates that the directory exists, and returns the absolute path.
func checkWatchDir(watch_dir string) string {
    _, err := os.Stat(watch_dir)
    if os.IsNotExist(err) {
        fmt.Println("Watch directory does not exist")
        os.Exit(1)
    }
    abs_dir, _ := filepath.Abs(watch_dir)
    return abs_dir
}
