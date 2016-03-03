package sender

import (
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"testing"
	"time"
	"util"
)

var error_status string
var test_file string

// receiveFileHandler is the HTTP request handler that is used to check the validity of the multipart request in
// both the SingleBin and MultiBin tests.
func receiveFileHandler(w http.ResponseWriter, r *http.Request) {
	error_status = "failed"
	boundary := r.Header.Get("Boundary")
	if len(boundary) < 1 {
		error_status = "Boundary header does not exist."
		return
	}
	reader := multipart.NewReader(r.Body, boundary)
	for {
		next_part, eof := reader.NextPart()
		if eof != nil && eof != io.EOF {
			error_status = eof.Error()
			return
		}
		if eof == io.EOF {
			error_status = ""
			return
		}
		part_contents, read_err := ioutil.ReadAll(next_part)
		if read_err != nil && read_err != io.ErrUnexpectedEOF {
			error_status = "Failed to read sent part " + read_err.Error()
			return
		}
		file_contents, _ := ioutil.ReadFile(next_part.Header.Get("name"))
		if string(part_contents) != string(file_contents) {
			error_status = "Sent part data does not match data in file"
			return
		}
	}
}

// TestSingleBin creates a bin with a single file, and asks the sender to send it.
// It checks if the resulting sent request contains the same data that the sender was given.
func TestSingleBin(t *testing.T) {
	config = util.Config{}
	config.Server_Port = "8080"
	config.Sender_Server_Port = "8080"
	config.Receiver_Address = "localhost:8081"
	config.Client_SSL_Cert = "../conf/client.pem"
	config.Client_SSL_Key = "../conf/client.key"
	// Create and pass new bin to sender
	error_status = ""
	test_file = "send_test.txt"
	bin_queue := make(chan Bin, 1)
	sender := NewSender(nil, bin_queue, false)
	go sender.run()
	new_bin := NewBin(cache, 3000, "test")
	info, err := os.Stat(test_file)
	if err != nil {
		t.Error(err.Error())
		return
	}
	// Start webserver
	http.HandleFunc("/send.go", receiveFileHandler)
	util.AsyncListenAndServeTLS(":8081", config.Client_SSL_Cert, config.Client_SSL_Key)
	new_bin.addPart(test_file, 0, 60, info)
	bin_queue <- new_bin
	time.Sleep(200 * time.Millisecond) // Time the sender is allowed to use to get the file
	if len(error_status) > 1 {
		t.Error(error_status)
	}
}

// TestMultiBin creates a bin with multiple files, and asks the sender to send it.
// If checks if the resulting sent request contains the same data that the sender was given.
func TestMultiBin(t *testing.T) {
	error_status = ""
	bin_queue := make(chan Bin, 1)
	sender := NewSender(nil, bin_queue, false)
	go sender.run()
	test_file1 := "send_test.txt"
	test_file2 := "send_test2.txt"
	new_bin := NewBin(cache, 3000, "test")
	stat1, _ := os.Stat(test_file1)
	stat2, _ := os.Stat(test_file2)

	new_bin.addPart(test_file1, 0, 60, stat1)
	new_bin.addPart(test_file2, 0, 65, stat2)
	bin_queue <- new_bin
	time.Sleep(200 * time.Millisecond) // Time the sender is allowed to use to get the file
	if len(error_status) > 1 {
		t.Error(error_status)
	}
}
