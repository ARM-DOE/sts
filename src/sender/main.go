package main

import (
    "fmt"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "os"
    "path/filepath"
    "strings"
    "time"
)

// main is the entry point for the sender program.
// It parses config values that are necessary during runtime, dispatches the listening and sending threads, and loops infinitely.
func main() {
    config_file := parseConfig()
    SENDER_COUNT := config_file.Sender_Threads
    CACHE_FILE_NAME := config_file.Cache_File_Name
    WATCH_DIRECTORY := checkWatchDir(config_file.Directory)

    // Create the channel through which new bins will be sent from the sender to the receiver.
    bin_channel := make(chan Bin, SENDER_COUNT+5) // Create a bin channel with buffer size large enough to accommodate all sender threads and a little wiggle room.
    // Create and start cache file handler and webserver
    file_cache := CacheFactory(CACHE_FILE_NAME, WATCH_DIRECTORY, bin_channel, config_file.Bin_Size)
    file_cache.listener.LoadCache()
    server := WebserverFactory(file_cache, WATCH_DIRECTORY, config_file.Bin_Size)
    go server.startServer()

    // Dispatch senders
    senders := make([]*Sender, SENDER_COUNT)
    for dispatched_senders := 0; dispatched_senders < SENDER_COUNT; dispatched_senders++ {
        created_sender := SenderFactory(file_cache.bin_channel, config_file.Compression)
        go created_sender.run()
        senders[dispatched_senders] = created_sender
    }
    file_cache.SetSenders(senders)
    go file_cache.scan() // Start the listener thread
    fmt.Println("Ready to send")
    for {
        time.Sleep(1 * time.Second)
    }
}

// Config is the struct that all values from the configuration file are loaded into when it is parsed.
type Config struct {
    Directory               string
    Sender_Threads          int
    Log_File_Duration_Hours int
    Cache_File_Name         string
    Bin_Size                int64
    Compression             bool
}

// parseConfig parses the config.yaml file and returns the parsed results as an instance of the Config struct.
func parseConfig() Config {
    var loaded_config Config
    abs_path, _ := filepath.Abs("config.yaml")
    config_fi, config_err := ioutil.ReadFile(abs_path)
    if config_err != nil {
        fmt.Println("No config.yaml file found")
        os.Exit(1)
    }
    err := yaml.Unmarshal(config_fi, &loaded_config)
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
    return loaded_config
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
