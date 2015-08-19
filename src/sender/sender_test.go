package main

import (
    "io"
    "io/ioutil"
    "mime/multipart"
    "net/http"
    "os"
    "testing"
    "time"
)

var error_status string
var test_file string

func receiveFileHandler(w http.ResponseWriter, r *http.Request) {
    boundary := r.Header.Get("Boundary")
    if len(boundary) < 1 {
        error_status = "Boundary header does not exist."
        return
    }
    reader := multipart.NewReader(r.Body, boundary)
    for {
        next_part, eof := reader.NextPart()
        if eof != io.EOF {
            error_status = eof.Error()
            return
        }
        if eof == io.EOF {
            break
        }
        part_contents, read_err := ioutil.ReadAll(next_part)
        if read_err != nil {
            error_status = "Failed to read sent part"
            return
        }
        file_contents, _ := ioutil.ReadFile(next_part.Header.Get("name"))
        if string(part_contents) != string(file_contents) {
            error_status = "Sent part data does not match data in file"
            return
        }
    }
}

func TestSingleBin(t *testing.T) {
    // Create and pass new bin to sender
    error_status = ""
    wd, _ := os.Getwd()
    test_file = wd + string(os.PathSeparator) + "test_dir" + string(os.PathSeparator) + "send_test.txt"
    bin_queue := make(chan Bin, 1)
    sender := SenderFactory(bin_queue, false)
    go sender.run()
    new_bin := BinFactory(3000, "test_dir")
    info, err := os.Stat(test_file)
    if err != nil {
        t.Error(err.Error())
    }
    // Start webserver
    http.HandleFunc("/send.go", receiveFileHandler)
    go http.ListenAndServe(":8081", nil)
    new_bin.addPart(test_file, 0, 60, info)
    bin_queue <- new_bin
    time.Sleep(200 * time.Millisecond) // Time the sender is allowed to use to get the file
    if len(error_status) > 1 {
        t.Error(error_status)
    }
}

func TestMultiBin(t *testing.T) {
    error_status = ""
    wd, _ := os.Getwd()
    bin_queue := make(chan Bin, 1)
    sender := SenderFactory(bin_queue, false)
    go sender.run()
    test_file1 := wd + string(os.PathSeparator) + "test_dir" + string(os.PathSeparator) + "send_test.txt"
    test_file2 := wd + string(os.PathSeparator) + "test_dir" + string(os.PathSeparator) + "send_test2.txt"
    new_bin := BinFactory(3000, "test_dir")
    stat1, _ := os.Stat(test_file1)
    stat2, _ := os.Stat(test_file2)
    new_bin.addPart(test_file1, 0, 60, stat1)
    new_bin.addPart(test_file2, 0, 60, stat2)
    bin_queue <- new_bin
    time.Sleep(200 * time.Millisecond) // Time the sender is allowed to use to get the file
    if len(error_status) > 1 {
        t.Error(error_status)
    }
}
