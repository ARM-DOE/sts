package main

import (
	"fmt"
	//"strconv"
	"net/http"
	"os"
	"strings"
	"time"
)

type Sender struct {
	queue *Queue
}

func SenderFactory(file_queue *Queue) *Sender {
	new_sender := &Sender{}
	new_sender.queue = file_queue
	return new_sender
}

func getPathFromJSON(file_info string) string {
	return strings.Split(strings.Split(file_info, `{"path": "`)[1], `", "`)[0]
}

func getSizeFromJSON(file_info string) int64 {
	//result, _ := strconv.Atoi(strings.Split(strings.Split(file_info, `{"size": "`)[1], `"`)[0])
	//return int64(result)
	return 400
}

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
