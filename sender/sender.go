package main

import (
    "bytes"
    "compress/gzip"
    "fmt"
    "io"
    "io/ioutil"
    "mime/multipart"
    "net/http"
    "net/textproto"
    "os"
    "time"
    "util"
)

// Sender is a struct that continually requests new Bins from a channel.
// When an available Bin is found, Sender converts the Bin to a multipart file, which it then transmits to the reciever.
type Sender struct {
    queue       chan Bin
    compression bool
    Busy        bool
}

// SenderFactory creates and returns a new instance of the Sender struct.
// It takes a Bin channel as an argument, which the Sender uses to receiver newly filled or loaded Bins.
func SenderFactory(file_queue chan Bin, compression bool) *Sender {
    new_sender := &Sender{}
    new_sender.queue = file_queue
    new_sender.compression = compression
    return new_sender
}

// run is the mainloop of the sender struct. It blocks until it receives a Bin from the bin channel.
// Once the Sender receives a Bin, it creates the body of the file and sends it.
// After sending is verified to be complete, the Bin is deleted.
func (sender *Sender) run() {
    for {
        send_bin := <-sender.queue
        sender.Busy = true
        fmt.Println("Sending bin of size ", send_bin.Size)
        for index, _ := range send_bin.Files {
            send_bin.Files[index].getMD5()
        }
        bin_body := CreateBinBody(send_bin)
        bin_body.compression = sender.compression
        request, err := http.NewRequest("PUT", "http://localhost:8081/send.go", bin_body)
        request.Header.Add("Boundary", bin_body.Boundary())
        request.ContentLength = bin_body.getContentLength()
        if bin_body.compression {
            request.Header.Add("Content-Encoding", "gzip")
        }
        if err != nil {
            fmt.Println(err)
        }
        client := http.Client{}
        response, sending_err := client.Do(request)
        if sending_err != nil {
            fmt.Println(sending_err.Error())
            time.Sleep(5 * time.Second) // Wait so you don't choke the Bin queue if it keeps failing in quick succession.
            sender.queue <- send_bin    // Pass the bin back into the Bin queue.
        } else {
            response_code, _ := ioutil.ReadAll(response.Body)
            if string(response_code) == "200" {
                send_bin.delete() // Sending is complete, so remove the bin file
            }
        }
        sender.Busy = false
    }
}

// BinBody is a struct that, given a Bin, returns a portion of the contents in the Bin (and multipart headers) with every call to Read()
// BinBody implements io.Reader so that it can be passed to an HTTP request.
type BinBody struct {
    bin           Bin
    writer        *multipart.Writer
    writer_buffer bytes.Buffer // The buffer that the multipart.Writer will write headers to upon part creation.
    bin_part      Part         // The instance of the Part currently being operated on from Bin.Files
    file_handle   *os.File     // The file handle of the currently open File that corresponds to bin_part
    file_index    int          // The index of the Part currently being operated on from Bin.Files
    part_progress int64        // A sum of the byte counts read from the current file.
    eof_returned  bool         // Set to true when an EOF is returned so that further calls to read do not cause an error.
    compression   bool         // Set to true if you want to enable Bin compression.
    gzip_writer   *gzip.Writer // Gzip writer for writing compressed bytes if compression is enabled.
    gzip_buffer   bytes.Buffer
}

// CreateBinBody creates a new instance of a BinBody from an instance of Bin.
func CreateBinBody(bin Bin) *BinBody {
    if len(bin.Files) < 1 {
        panic("Tried to convert empty Bin to bytes")
    }
    new_body := &BinBody{}
    new_body.eof_returned = false
    new_body.compression = false
    new_body.gzip_buffer = bytes.Buffer{}
    new_body.gzip_writer, _ = gzip.NewWriterLevel(&new_body.gzip_buffer, gzip.BestCompression)
    new_body.bin = bin
    new_body.writer_buffer = bytes.Buffer{}
    new_body.file_index = 0
    new_body.writer = multipart.NewWriter(&new_body.writer_buffer)
    return new_body
}

// getContentLength uses the location headers in the Bin + extra multipart data to calculate the total content length of the Bin.
func (body *BinBody) getContentLength() int64 {
    content_length := int64(0)
    for _, element := range body.bin.Files {
        content_length += element.Start - element.End
    }
    content_length += int64(len(body.getClosingBoundary()))
    return content_length
}

// Boundary returns the multipart boundary from the BinBody.writer object.
func (body *BinBody) Boundary() string {
    return body.writer.Boundary()
}

// SetBoundary sets the boundary string in the BinBody instance of multipart writer.
func (body *BinBody) SetBoundary(boundary string) {
    body.writer.SetBoundary(boundary)
}

// getClosingBoundary returns the string that signifies the end of a multipart file.
func (body *BinBody) getClosingBoundary() string {
    return fmt.Sprintf("--%s--", body.Boundary())
}

// startNextPart is called when the size of the part is read or EOF is reached in the part file.
// When a new part is started, startNextPart() returns the header for that part.
func (body *BinBody) startNextPart() ([]byte, error) {
    if len(body.bin.Files) == body.file_index { // If the file index will cause an error next time it is used for slicing, the Bin is finished processing.
        body.eof_returned = true
        return nil, io.EOF
    }
    body.part_progress = 0
    body.bin_part = body.bin.Files[body.file_index]
    body.file_handle, _ = os.Open(body.bin_part.Path)
    body.file_handle.Seek(body.bin_part.Start, 0)
    new_header := textproto.MIMEHeader{}
    new_header.Add("Content-Disposition", "form-data")
    new_header.Add("Content-Type", "application/octet-stream")
    new_header.Add("md5", body.bin_part.MD5)
    new_header.Add("name", getStorePath(body.bin_part.Path, body.bin.WatchDir))
    new_header.Add("total_size", fmt.Sprintf("%d", body.bin_part.TotalSize))
    new_header.Add("location", getPartLocation(body.bin_part.Start, body.bin_part.End))
    body.writer.CreatePart(new_header)
    return body.writer_buffer.Bytes(), nil
}

// Read is BinBody's implementation of an io.Reader Read().
// If the bin isn't already finished processing, and no new parts need to be started, it reads a portion of the Bin file into file_buffer until every part has been completed.
func (body *BinBody) Read(file_buffer []byte) (int, error) {
    if body.eof_returned { // If the Bin is already done processing, return the closing boundary and EOF.
        ending_boundary := []byte(body.getClosingBoundary())
        copy(file_buffer[0:len(ending_boundary)], ending_boundary)
        return len(ending_boundary), io.EOF
    }
    // If this is the first call to Read, start the first part of the Bin and return the header.
    if body.file_index == 0 {
        initial_header, _ := body.startNextPart()
        body.file_index++
        copy(file_buffer, initial_header)
        return len(initial_header), nil
    }
    // Check to see that we're not going over max part size.
    bytes_left := (body.bin_part.End - body.bin_part.Start) - body.part_progress
    temp_buffer := file_buffer
    if bytes_left < int64(len(file_buffer)) {
        temp_buffer = make([]byte, bytes_left)
    }
    bytes_read, file_read_error := body.file_handle.Read(temp_buffer)
    // Do compression if enabled
    if body.compression {
        body.gzip_buffer.Reset()
        body.gzip_writer.Write(temp_buffer)
        body.gzip_writer.Flush()
        temp_buffer = body.gzip_buffer.Bytes()
        bytes_read = len(temp_buffer)
    }
    copy(file_buffer, temp_buffer)
    body.part_progress += int64(bytes_read)
    // If an EOF is encountered or the part is out of room, start a new part.
    if file_read_error != nil || bytes_left < int64(len(file_buffer)) {
        header_bytes, eof := body.startNextPart()
        body.file_index++
        if eof != nil {
            return bytes_read, nil
        } else {
            return len(header_bytes), eof
        }
    }
    return bytes_read, nil
}

// getBinBody generates and returns a multipart file based on the Parts defined in the Bin.
// getBinBody returns a byte array that contains the bytes of the multipart file, and a boundary string, which is needed to parse the multipart file.
// Currently not used, but can be used as a reference for correctly generating a multipart file.
func getBinBody(bin Bin) ([]byte, string) {
    body_buffer := bytes.Buffer{}
    multipart_writer := multipart.NewWriter(&body_buffer)
    multipart_writer.SetBoundary(multipart_writer.Boundary())
    for _, part := range bin.Files {
        fi, _ := os.Open(part.Path)
        part_bytes := make([]byte, part.End-part.Start)
        fi.Seek(part.Start, 0)
        fi.Read(part_bytes)
        part_header := textproto.MIMEHeader{}
        md5 := util.GenerateMD5(part_bytes)
        part_header.Add("md5", md5)
        part_header.Add("name", getStorePath(part.Path, bin.WatchDir))
        part_header.Add("total_size", fmt.Sprintf("%d", part.TotalSize))
        part_header.Add("location", getPartLocation(part.Start, part.End))
        new_part, _ := multipart_writer.CreatePart(part_header)
        new_part.Write(part_bytes)
    }
    multipart_writer.Close()
    return body_buffer.Bytes(), multipart_writer.Boundary()
}

// getPartLocation formats a string for sending as a header in each part.
// It takes two byte parameters. The first int64 represents the first byte of the part in the file, the second represents the size of the part.
func getPartLocation(start int64, end int64) string {
    return fmt.Sprintf("%d:%d", start, end)
}
