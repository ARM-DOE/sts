package sender

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
    "path/filepath"
    "time"
    "util"
)

// Sender is a struct that continually requests new Bins from a channel.
// When an available Bin is found, Sender converts the Bin to a multipart file,
// which it then transmits to the receiver.
type Sender struct {
    queue       chan Bin // Channel that new bins are pulled from.
    compression bool     // Bool that controls whether compression is turned on. Obtained from config file.
    Busy        bool     // Set to true when the sender is currently sending a file.
}

// NewSender creates and returns a new instance of the Sender struct.
// It takes a Bin channel as an argument, which the Sender uses to receiver newly filled or loaded Bins.
func NewSender(file_queue chan Bin, compression bool) *Sender {
    new_sender := &Sender{}
    new_sender.queue = file_queue
    new_sender.compression = compression
    return new_sender
}

// run is the main loop of the sender struct. It blocks until it receives a Bin from the bin channel.
// Once the Sender receives a Bin, it dispatches the function that corresponds to the transfer method
// of the Bin.
func (sender *Sender) run() {
    for {
        send_bin := <-sender.queue
        sender.Busy = true
        fmt.Println("Sending bin of size ", send_bin.Size)
        switch send_bin.TransferMethod {
        case TRANSFER_HTTP:
            sender.sendHTTP(send_bin)
        case TRANSFER_DISK:
            sender.sendDisk(send_bin)
        case TRANSFER_GRIDFTP:
            sender.sendGridFTP(send_bin)
        default:
            panic(fmt.Sprintf("Unknown Bin.TransferMethod %d in %s", send_bin.TransferMethod, send_bin.Name))
        }
        sender.Busy = false
    }
}

// sendHTTP accepts Bins with transfer type HTTP, creates a BinBody stream for the Bin, and streams
// the Bin body in an HTTP request to the receiver.
func (sender *Sender) sendHTTP(send_bin Bin) {
    for index, _ := range send_bin.Files {
        send_bin.Files[index].getMD5()
    }
    bin_body := CreateBinBody(send_bin)
    bin_body.compression = sender.compression
    request_url := fmt.Sprintf("http://%s/send.go", config.Receiver_Address)
    request, err := http.NewRequest("PUT", request_url, bin_body)
    request.Header.Add("Transfer-Encoding", "chunked")
    request.Header.Add("Boundary", bin_body.Boundary())
    request.ContentLength = -1
    if bin_body.compression {
        request.Header.Add("Content-Encoding", "gzip")
    }
    if err != nil {
        fmt.Println(err.Error())
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
}

// sendDisk accepts Bins with transfer type Disk, and copies them to a location on the system.
func (sender *Sender) sendDisk(send_bin Bin) {
    bin_part := send_bin.Files[0] // Bin will have only one part
    dest_path := util.JoinPath(config.Disk_Path, getStorePath(bin_part.Path, config.Directory))
    mkdir_err := os.MkdirAll(filepath.Dir(dest_path), os.ModePerm)
    if mkdir_err != nil {
        fmt.Println(mkdir_err.Error())
        return
    }
    dest_fi, dest_err := os.Create(dest_path)
    if dest_err != nil {
        fmt.Println(dest_err.Error())
        return
    }
    src_fi, src_err := os.Open(bin_part.Path)
    if src_err != nil {
        fmt.Println(src_err.Error())
        return
    }
    stream_md5 := util.NewStreamMD5()
    read_buffer := make([]byte, stream_md5.BlockSize)
    for {
        bytes_read, eof := src_fi.Read(read_buffer)
        stream_md5.Update(read_buffer[0:bytes_read])
        dest_fi.Write(read_buffer[0:bytes_read])
        if eof == io.EOF {
            break
        }
    }
    dest_fi.Close()
    util.NewCompanion(dest_path, bin_part.TotalSize)
    util.AddPartToCompanion(dest_path, stream_md5.SumString(), getPartLocation(0, bin_part.TotalSize))
    send_bin.delete() // Write finished, delete bin.
}

// sendGridFTP accepts Bins with transfer type GridFTP, and does nothing with them.
func (sender *Sender) sendGridFTP(send_bin Bin) {

}

// BinBody is a struct that, given a Bin, returns a portion of the contents in
// the Bin (and multipart headers) with every call to Read()
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
    unsent_header []byte
    gzip_writer   *gzip.Writer // Gzip writer for writing compressed bytes if compression is enabled.
    gzip_buffer   bytes.Buffer
}

// CreateBinBody creates a new instance of a BinBody from an instance of Bin.
// It takes the optional boundary argument, which sets the multipart writer boundary upon creation.
func CreateBinBody(bin Bin, boundary ...string) *BinBody {
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
    if len(boundary) > 0 {
        new_body.writer.SetBoundary(boundary[0])
    }
    new_body.startNextPart()
    return new_body
}

// startNextPart is called when the size of the part is read or EOF is reached in the part file.
// When a new part is started, startNextPart() returns the header for that part.
func (body *BinBody) startNextPart() {
    if len(body.bin.Files) == body.file_index { // If the file index will cause an error next time it is used for slicing, the Bin is finished processing.
        body.eof_returned = true
        return
    }
    body.part_progress = 0
    body.writer_buffer.Truncate(0)
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
    body.unsent_header = body.writer_buffer.Bytes()
    body.file_index++
}

// Read is BinBody's implementation of an io.Reader Read().
// If the bin isn't already finished processing, and no new parts need to be
// started, it reads a portion of the Bin file into file_buffer until every part has been completed.
// If a new part needs to be started, it will copy the new part header to file_buffer.
func (body *BinBody) Read(file_buffer []byte) (int, error) {
    if body.eof_returned {
        // Files are done, return closing boundary and EOF.
        closing_boundary := []byte(body.getClosingBoundary())
        copy(file_buffer[0:len(closing_boundary)], closing_boundary)
        return len(closing_boundary), io.EOF
    } else if body.unsent_header != nil {
        // A new part was started, send its header.
        copy(file_buffer[0:len(body.unsent_header)], body.unsent_header)
        header_len := len(body.unsent_header)
        body.unsent_header = nil
        return header_len, nil
    } else {
        // Read from file
        bytes_left := (body.bin_part.End - body.bin_part.Start) - body.part_progress
        temp_buffer := file_buffer
        if bytes_left < int64(len(file_buffer)) {
            temp_buffer = make([]byte, bytes_left) // If the amount that we want to read from the part is smaller than the buffer size, make a new buffer.
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
        if file_read_error != nil || bytes_left < int64(len(file_buffer)) {
            body.startNextPart()
        }
        return bytes_read, nil
    }
    return 0, nil
}

// Boundary returns the multipart boundary from the BinBody.writer object.
func (body *BinBody) Boundary() string {
    return body.writer.Boundary()
}

// getClosingBoundary returns the string that signifies the end of a multipart file.
func (body *BinBody) getClosingBoundary() string {
    return fmt.Sprintf("--%s--", body.Boundary())
}

// getPartLocation formats a string for sending as a header in each part.
// It takes two byte parameters. The first int64 represents the first byte
// of the part in the file, the second represents the last byte of the part.
func getPartLocation(start int64, end int64) string {
    return fmt.Sprintf("%d:%d", start, end)
}
