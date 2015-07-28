package main

import (
    "fmt"
    "net/http"
    "os"
    "strings"
    "time"
)

// Sender is a data structure that continually requests new items from a Queue.
// When an available item is found, Sender transmits the file to the receiver.
type Sender struct {
    queue *Queue
}

// SenderFactory creates and returns a new instance of the Sender struct.
func SenderFactory(file_queue *Queue) *Sender {
    new_sender := &Sender{}
    new_sender.queue = file_queue
    return new_sender
}

// getPathFromJSON is a temporary parsing function that retrieves the file path on the system of the sender from a JSON string.
func getPathFromJSON(file_info string) string {
    return strings.Split(strings.Split(file_info, `{"path": "`)[1], `", "`)[0]
}

// run is the mainloop of the sender struct. It requests new data from the Queue once every second.
// If it receives a JSON string from the Queue, it will send the specified file to the receiver.
func (sender *Sender) run() {
    for true {
        request_response := sender.queue.request()
        if len(request_response) > 0 {
            fmt.Println("Sending " + getPathFromJSON(request_response))
            fi, _ := os.Open(getPathFromJSON(request_response))
            request, _ := http.NewRequest("PUT", "http://localhost:8080/send.go", fi)
            request.Header.Add("metadata", request_response)
            client := http.Client{}
            client.Do(request)
        }
        time.Sleep(time.Duration(1) * time.Second)
    }
}
