package main

import (
    "crypto/md5"
    "fmt"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "time"
)

// main is the entry point for the sender program.
// It parses config values that are necessary during runtime, and dispatches the listening and sending threads, after which it loops infinitely.
func main() {
    fmt.Println(os.Getwd())
    config_file := parseConfig()
    SENDER_COUNT, _ := strconv.Atoi(config_file["SenderThreads"])
    CACHE_FILE_NAME := config_file["CacheFileName"]
    WATCH_DIRECTORY := checkWatchDir(config_file["Directory"])

    // Create the channel through which new bins will be sent from the sender to the
    bin_channel := make(chan Bin, 1)
    // Create and start cache file handler and webserver
    file_cache := CacheFactory(CACHE_FILE_NAME, WATCH_DIRECTORY, bin_channel)
    file_cache.loadCache()
    server := WebserverFactory()
    go server.startServer()

    // Dispatch senders
    dispatched_senders := 0
    sender_delay := time.Duration(1000 / SENDER_COUNT)
    senders := make([]*Sender, 0, SENDER_COUNT)
    for dispatched_senders < SENDER_COUNT {
        created_sender := SenderFactory(file_cache.bin_channel)
        go created_sender.run()
        senders = append(senders, created_sender)
        dispatched_senders++
        time.Sleep(sender_delay * time.Millisecond)
    }
    fmt.Println("Senders dispatched")
    go file_cache.listen() // Start the listener thread
    for {
        time.Sleep(1 * time.Second)
    }
}

// parseConfig parses the config.yaml file and returns the parsed results as a map of strings.
func parseConfig() map[string]string {
    var parsed_yaml map[string]string
    abs_path, _ := filepath.Abs("config.yaml")
    config_fi, confg_err := ioutil.ReadFile(abs_path)
    if confg_err != nil {
        fmt.Println("No config.yaml file found")
        os.Exit(1)
    }
    err := yaml.Unmarshal(config_fi, &parsed_yaml)
    if err != nil {
        fmt.Println("Error parsing configuration file")
        os.Exit(1)
    }
    return parsed_yaml
}

// generateMD5 generates and returns an md5 string from an array of bytes.
func generateMD5(data []byte) string {
    new_hash := md5.New()
    new_hash.Write(data)
    return fmt.Sprintf("%x", new_hash.Sum(nil))
}

// getStorePath returns the path that the receiver should use to store a file.
// Given parameters full_path and watch_directory, it will remove watch directory from the full path.
func getStorePath(full_path string, watch_directory string) string {
    store_path := strings.Replace(full_path, filepath.Dir(watch_directory)+"/", "", 1)
    return store_path
}

// checkWatchDir is called on the directory to be watched.
// It validates that the directory exists, and returns the absolute path.
func checkWatchDir(watch_dir string) string {
    _, err := os.Open(watch_dir)
    if err != nil {
        fmt.Println("Directory does not exist")
        os.Exit(1)
    }
    watch_dir, _ = filepath.Abs(watch_dir)
    return watch_dir
}

// inArray takes an array of strings and a string value, and returns a boolean.
// true if the value is equal to a value in the array, else false.
func inArray(array []string, value string) bool {
    for _, element := range array {
        if element == value {
            return true
        }
    }
    return false
}
