package main

import (
    "fmt"
    "os"
    "time"
)

// main is the entry point for the sender program.
// It defines constants such as sender count, and dispatches the listening and sending threads, after which it loops infinitely
// Dispatching of senders is initially staggered to space out requests to the queue.
func main() {
    const SENDER_COUNT = 10
    var WATCH_DIRECTORY string
    fmt.Print("Directory to watch: ")
    fmt.Scan(&WATCH_DIRECTORY)
    _, err := os.Open(WATCH_DIRECTORY)
    if err != nil {
        fmt.Println("Directory does not exist")
        main()
    }

    direc_listener := ListenerFactory(WATCH_DIRECTORY)
    go direc_listener.listen()

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
        time.Sleep(10 * time.Second)
    }
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
