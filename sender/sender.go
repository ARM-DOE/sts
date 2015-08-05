package main

import (
    "bytes"
    "fmt"
    "mime/multipart"
    "net/http"
    "net/textproto"
    "os"
    "time"
    "util"
)

// Sender is a data structure that continually requests new Bins from a channel.
// When an available Bin is found, Sender converts the Bin to a multipart file, which it then transmits to the reciever.
type Sender struct {
    queue      chan Bin
    chunk_size int64
}

// SenderFactory creates and returns a new instance of the Sender struct.
// It takes a Bin channel as an argument, which the Sender uses to receiver newly filled or loaded Bins.
func SenderFactory(file_queue chan Bin) *Sender {
    new_sender := &Sender{}
    new_sender.queue = file_queue
    new_sender.chunk_size = 100
    return new_sender
}

// run is the mainloop of the sender struct. It blocks until it receives a Bin from the bin channel.
// Once the Sender receives a Bin, it creates the body of the file and sends it.
// After sending is complete, the Bin is deleted.
func (sender *Sender) run() {
    for {
        select {
        case send_bin := <-sender.queue:
            fmt.Println("Sending Bin of size ", send_bin.Size)
            bytes_to_send, boundary := getBinBody(send_bin)
            byte_reader := bytes.NewReader(bytes_to_send)
            request, _ := http.NewRequest("PUT", "http://localhost:8081/send.go", byte_reader)
            request.Header.Add("boundary", boundary)
            client := http.Client{}
            response, sending_err := client.Do(request)
            if sending_err != nil {
                fmt.Println(sending_err.Error(), response)
                time.Sleep(5 * time.Second) // Wait so you don't choke the Bin queue if it keeps failing in quick succession.
                sender.queue <- send_bin    // Pass the bin back into the Bin queue.
            } else {
                send_bin.delete() // Sending is complete, so remove the bin file
            }
        }
    }
}

// getBinBody generates and returns a multipart file based on the Parts defined in the Bin.
// getBinBody returns a byte array that contains the bytes of the multipart file, and a boudnary string, which is needed to parse the multipart file.
func getBinBody(bin Bin) ([]byte, string) {
    body_buffer := bytes.Buffer{}
    multipart_writer := multipart.NewWriter(&body_buffer)
    multipart_writer.SetBoundary(util.MULTIPART_BOUNDARY)
    for _, part := range bin.Files {
        fi, _ := os.Open(part.Path)
        chunk_bytes := make([]byte, part.End-part.Start)
        fi.Seek(part.Start, 0)
        fi.Read(chunk_bytes)
        chunk_header := textproto.MIMEHeader{}
        md5 := util.GenerateMD5(chunk_bytes)
        chunk_header.Add("md5", md5)
        chunk_header.Add("name", getStorePath(part.Path, bin.WatchDir))
        chunk_header.Add("total_size", fmt.Sprintf("%d", part.TotalSize))
        chunk_header.Add("location", getChunkLocation(part.Start, part.End))
        new_part, _ := multipart_writer.CreatePart(chunk_header)
        new_part.Write(chunk_bytes)
    }
    multipart_writer.Close()
    return body_buffer.Bytes(), multipart_writer.Boundary()
}

// getChunkLocation formats a string for sending as an HTTP header.
// It takes two byte parameters. The first int64 represents the first byte of the chunk in the file, the second represents the size of the chunk.
func getChunkLocation(start int64, end int64) string {
    return fmt.Sprintf("%d:%d", start, end)
}
