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
    queue      chan string
    chunk_size int64
}

// SenderFactory creates and returns a new instance of the Sender struct.
func SenderFactory(file_queue chan string) *Sender {
    new_sender := &Sender{}
    new_sender.queue = file_queue
    new_sender.chunk_size = 100
    return new_sender
}

// getPathFromJSON is a temporary parsing function that retrieves the file path on the system of the sender from a JSON string.
func getPathFromJSON(file_info string) string {
    return strings.Split(strings.Split(file_info, `{"path": "`)[1], `", "`)[0]
}

// run is the mainloop of the sender struct. It blocks until it receives data from the file queue channel.
// If it receives a JSON string from the Queue, it will send the specified file to the receiver as a multipart file.
func (sender *Sender) run() {
    for {
        select {
        case request_response := <-sender.queue:
            fmt.Println("Sending " + getPathFromJSON(request_response))
            chunked_file, boundary := chunkFile(getPathFromJSON(request_response), sender.chunk_size)
            byte_reader := bytes.NewReader(chunked_file)
            request, _ := http.NewRequest("PUT", "http://localhost:8081/send.go", byte_reader)
            request.Header.Add("metadata", request_response)
            request.Header.Add("boundary", boundary)
            request.Header.Add("chunk_size", fmt.Sprintf("%d", sender.chunk_size))
            client := http.Client{}
            client.Do(request)
        }
    }
}

// getChunkLocation formats a string for sending as an HTTP header.
// It takes two byte parameters. The first int64 represents the first byte of the chunk in the file, the second represents the size of the chunk.
func getChunkLocation(start int64, chunk_size int64) string {
    return fmt.Sprintf("%d:%d", start, start+chunk_size)
}

// chunkFile takes a filepath and chunk_size (in bytes) to split a file into a mime/multipart file.
// chunkFile also calculates the MD5 for each file chunk and adds it as a header of that chunk
func chunkFile(path string, chunk_size int64) ([]byte, string) {
    info, _ := os.Stat(path)
    fi, _ := os.Open(path)
    byte_buffer := bytes.Buffer{}
    multipart_writer := multipart.NewWriter(&byte_buffer)
    for chunks := int64(0); chunks < (info.Size()/chunk_size)+1; chunks++ {
        chunk_from_file := make([]byte, chunk_size)
        fi.Read(chunk_from_file)
        chunk_header := textproto.MIMEHeader{}
        md5 := generateMD5(chunk_from_file)
        chunk_header.Add("md5", md5)
        chunk_header.Add("name", path)
        chunk_header.Add("location", getChunkLocation(chunks*chunk_size, chunk_size))
        part_writer, _ := multipart_writer.CreatePart(chunk_header)
        part_writer.Write(chunk_from_file)
    }
    multipart_writer.Close()
    return byte_buffer.Bytes(), multipart_writer.Boundary()
}
