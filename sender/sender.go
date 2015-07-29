package main

import (
    "bytes"
    "fmt"
    "mime/multipart"
    "net/http"
    "net/textproto"
    "os"
    "strings"
)

// Sender is a data structure that continually requests new items from a Queue.
// When an available item is found, Sender transmits the file to the receiver.
type Sender struct {
    queue chan string
}

// SenderFactory creates and returns a new instance of the Sender struct.
func SenderFactory(file_queue chan string) *Sender {
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
        select {
        case request_response := <-sender.queue:
            fmt.Println("Sending " + getPathFromJSON(request_response))
            fi, _ := os.Open(getPathFromJSON(request_response))
            request, _ := http.NewRequest("PUT", "http://localhost:8081/send.go", fi)
            request.Header.Add("metadata", request_response)
            client := http.Client{}
            client.Do(request)
        }
    }
}

// chunkFile takes a filepath and chunk_size (in bytes) to split a file into a mime/multipart file.
// chunkFile also calculates the MD5 for each file chunk and adds it as a header of that chunk
func chunkFile(path string, chunk_size int64) ([]byte, string) {
    info, _ := os.Stat(path)
    fi, _ := os.Open(path)
    byte_buffer := bytes.Buffer{}
    multipart_writer := multipart.NewWriter(&byte_buffer)
    for chunks := int64(0); chunks < info.Size()/chunk_size; chunks++ {
        chunk_from_file := make([]byte, chunk_size)
        fi.Read(chunk_from_file)
        chunk_header := textproto.MIMEHeader{}
        chunk_header.Add("md5", generateMD5(chunk_from_file))
        part_writer, _ := multipart_writer.CreatePart(chunk_header)
        part_writer.Write(chunk_from_file)
    }
    multipart_writer.Close()
    return byte_buffer.Bytes(), multipart_writer.Boundary()
}

// validateMultipartFile is a proof of concept function which is capable of reading multipart files outputted by chunkFile.
// This function would actually be executed on the receiver.
// For each chunk in the multipart file, the md5 is verified, if a bad chunk is detected, the chunk could be reobtained from the webserver in the sender.
func validateMultipartFile(file_bytes []byte, boundary string, chunk_size int64) bool {
    file_is_good := true
    multipart_reader := multipart.NewReader(bytes.NewReader(file_bytes), boundary)
    for {
        next_part, end_of_file := multipart_reader.NextPart()
        if end_of_file != nil { // Reached end of multipart file
            return file_is_good
        }
        bytes_of_chunk := make([]byte, chunk_size)
        next_part.Read(bytes_of_chunk)
        if next_part.Header.Get("md5") != generateMD5(bytes_of_chunk) { // calculate md5 again, using what was given
            // validation failed, request this specific chunk again using chunk size
            fmt.Println("Bad md5")
            file_is_good = false
        }
    }
    return file_is_good
}
