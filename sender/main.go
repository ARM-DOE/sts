package main

import (
    "bytes"
    "crypto/md5"
    "fmt"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "os"
    "path/filepath"
    "strconv"
    "time"
)

// main is the entry point for the sender program.
// It defines constants such as sender count, and dispatches the listening and sending threads, after which it loops infinitely
// Dispatching of senders is initially staggered to space out requests to the queue.
func main() {
    config_file := parseConfig()
    SENDER_COUNT, _ := strconv.Atoi(config_file["SenderThreads"])
    CACHE_FILE_NAME := config_file["CacheFileName"]
    WATCH_DIRECTORY := checkWatchDir(config_file["Directory"])

    // Create and start cache scan
    file_cache := CacheFactory(CACHE_FILE_NAME, WATCH_DIRECTORY)
    file_cache.loadCache()
    file_cache.scanDir()

    direc_listener := ListenerFactory(WATCH_DIRECTORY, file_cache)
    go direc_listener.listen()
    server := WebserverFactory()
    go server.startServer() // Starts webserver

    // Dispatch senders
    dispatched_senders := 0
    sender_delay := time.Duration(1000 / SENDER_COUNT)
    senders := make([]*Sender, 0, SENDER_COUNT)
    for dispatched_senders < SENDER_COUNT {
        created_sender := SenderFactory(direc_listener.file_queue)
        go created_sender.run()
        senders = append(senders, created_sender)
        dispatched_senders++
        time.Sleep(sender_delay * time.Millisecond)
    }
    fmt.Println("Senders dispatched")

    for true {
        time.Sleep(1000 * time.Millisecond)
    }
}

// parseConfig parses the config.yaml file and returns the parsed results as a map of strings.
func parseConfig() map[string]string {
    var parsed_yaml map[string]string
    config_fi, confg_err := ioutil.ReadFile("config.yaml")
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

func generateMD5(data []byte) string {
    new_hash := md5.New()
    bytes.NewBuffer(data).WriteTo(new_hash)
    return string(new_hash.Sum(nil))
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
