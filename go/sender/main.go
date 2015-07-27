package main

import (
	"fmt"
	"os"
	"time"
)

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

func inArray(array []string, value string) bool {
	for _, element := range array {
		if element == value {
			return true
		}
	}
	return false
}
