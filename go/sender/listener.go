package main

import (
	"crypto/md5"
	"fmt"
	"github.com/rjeczalik/notify"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

type Listener struct {
	directory          string
	walked_directories []string
	event_channel      chan notify.EventInfo
	file_queue         *Queue
	watch_directory    string
}

func ListenerFactory(watch_directory string) *Listener {
	directory_list := make([]string, 0, 10)
	file_queue := QueueFactory()
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
	store_path := strings.Replace(file_path, listener.watch_directory+"/", "", 1)
	json_summary := fmt.Sprintf(`{"file": {"path": "%s", "md5": "%x", "size": "%d", "modtime": "%s", "store_path": "%s"}}`, file_path, md5_hash.Sum(nil), info.Size(), info.ModTime(), store_path)
	listener.file_queue.add(json_summary)
	fmt.Println(json_summary)
}

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
