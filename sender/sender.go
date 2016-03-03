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

	"github.com/ARM-DOE/sts/util"
)

// Sender is a struct that continually requests new Bins from a channel.
// When an available Bin is found, Sender converts the Bin to a multipart file,
// which it then transmits to the receiver.
type Sender struct {
	Busy         bool              // Set to true when the sender is currently sending a file.
	Active       bool              // Set Active to true if the sender is allowed to accept new Bins.
	queue        chan Bin          // Channel that new bins are pulled from.
	compression  bool              // Bool that controls whether compression is turned on. Obtained from config file.
	disk_manager *util.DiskManager // Disk manager for linux, keeps the disk from overfilling & controls disk mounting.
	nfailures    int               // Number of sequential failures used to gauge thrashing.
	client       http.Client
}

// NewSender creates and returns a new instance of the Sender struct.
// It takes a Bin channel as an argument, which the Sender uses to receiver newly filled or loaded Bins.
func NewSender(disk_manager *util.DiskManager, file_queue chan Bin, compression bool) *Sender {
	new_sender := &Sender{}
	new_sender.Active = true
	new_sender.disk_manager = disk_manager
	new_sender.queue = file_queue
	new_sender.compression = compression
	var client_err error
	new_sender.client, client_err = util.GetTLSClient(config.Client_SSL_Cert, config.Client_SSL_Key, config.TLS)
	if client_err != nil {
		util.LogError(client_err.Error())
	}
	new_sender.client.Timeout = time.Second * time.Duration(config.Bin_Timeout)
	return new_sender
}

// run is the main loop of the sender struct. It blocks until it receives a Bin from the bin channel.
// Once the Sender receives a Bin, it dispatches the function that corresponds to the transfer method
// of the Bin.
func (sender *Sender) run() {
	for {
		if !sender.Active {
			time.Sleep(1 * time.Second)
			continue
		}
		send_bin := <-sender.queue
		sender.Busy = true
		util.LogDebug("SENDER Bin:", send_bin.Name, send_bin.Size)
		switch send_bin.TransferMethod {
		case TRANSFER_HTTP:
			sender.sendHTTP(send_bin)
		case TRANSFER_DISK:
			sender.sendDisk(send_bin)
		case TRANSFER_GRIDFTP:
			sender.sendGridFTP(send_bin)
		default:
			util.LogError("Unknown Bin transfer method", send_bin.TransferMethod, "in", send_bin.Name)
			panic("Fatal error")
		}
		sender.Busy = false
	}
}

// sendHTTP accepts Bins with transfer type HTTP, creates a BinBody stream for the Bin, and streams
// the Bin body in an HTTP request to the receiver.
func (sender *Sender) sendHTTP(send_bin Bin) {
	for index, _ := range send_bin.Files {
		md5, md5_err := send_bin.Files[index].getMD5()
		send_bin.Files[index].MD5 = md5
		if md5_err != nil {
			util.LogError(md5_err.Error())
		}
	}
	bin_body := NewBinBody(send_bin)
	bin_body.compression = config.Compression()
	request_url := fmt.Sprintf("%s://%s/data", config.Protocol(), config.Receiver_Address)
	request, err := http.NewRequest("PUT", request_url, bin_body)
	request.Header.Add("X-STS-SenderName", config.Sender_Name)
	request.Header.Add("Transfer-Encoding", "chunked")
	request.Header.Add("Boundary", bin_body.Boundary())
	request.Header.Set("Connection", "close") // Prevents persistent connections opening too many file handles
	request.ContentLength = -1
	if bin_body.compression {
		request.Header.Add("Content-Encoding", "gzip")
	}
	if err != nil {
		util.LogError(err.Error())
	}
	response, sending_err := sender.client.Do(request)
	if sending_err != nil {
		util.LogError(sending_err.Error())
		sender.passBackBin(send_bin)
	} else {
		response_code, read_err := ioutil.ReadAll(response.Body)
		if read_err != nil {
			util.LogError("Could not read HTTP response from receiver:", read_err.Error())
			sender.passBackBin(send_bin)
		} else if string(response_code) == "200" {
			sender.nfailures = 0 // Reset the sequential failures counter
			send_bin.done()
		} else if string(response_code) == "206" {
			util.LogError("Bin failed validation:", send_bin.Name)
			sender.passBackBin(send_bin) // Bin failed validation on receiving end
		} else {
			util.LogError("Send failed with response code:", string(response_code))
			sender.passBackBin(send_bin)
		}
		response.Body.Close()
		request.Close = true
	}
}

// passBackBin should be called when a sender has failed to send a bin and
// needs to return it to the bin queue to be tried again by other senders.
func (sender *Sender) passBackBin(send_bin Bin) {
	sender.nfailures += 1
	if sender.nfailures > 3 {
		time.Sleep(time.Duration(sender.nfailures) * time.Second)
	}
	sender.queue <- send_bin // Pass the bin back into the Bin queue.
}

// sendDisk accepts Bins with transfer type Disk, and copies them to a location on the system
// after receiving permission from the disk manager.
func (sender *Sender) sendDisk(send_bin Bin) {
	bin_part := send_bin.Files[0] // Bin will have only one part
	// Ask the disk manager whether we can begin writing
	if !sender.disk_manager.CanWrite(bin_part.TotalSize) {
		sender.disk_manager.WaitForWrite()
	}
	// Let the manager know that we're writing a file
	sender.disk_manager.Writing(bin_part.TotalSize)
	// Prep the directory and create the file
	dest_path := util.JoinPath(config.Disk_Path, getStorePath(bin_part.Path, config.Input_Directory))
	mkdir_err := os.MkdirAll(filepath.Dir(dest_path), os.ModePerm)
	if mkdir_err != nil {
		util.LogError(mkdir_err.Error())
		return
	}
	dest_fi, dest_err := os.Create(dest_path)
	if dest_err != nil {
		util.LogError(dest_err.Error())
		return
	}
	src_fi, src_err := os.Open(bin_part.Path)
	if src_err != nil {
		util.LogError(src_err.Error())
		return
	}

	// Try to write the file to disk
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
	// Tell the disk manager that we're done writing to disk.
	sender.disk_manager.DoneWriting(bin_part.TotalSize)
	_, comp_err := util.NewCompanion(dest_path, bin_part.TotalSize)
	if comp_err != nil {
		util.LogError("Could not create new companion:", comp_err.Error())
	}
	add_err := util.AddPartToCompanion(dest_path, stream_md5.SumString(), getPartLocation(0, bin_part.TotalSize), stream_md5.SumString(), bin_part.Last_File)
	if add_err != nil {
		util.LogError("Failed to add part to companion:", add_err.Error())
	}
	// Tell receiver that we wrote a file to disk
	post_url := fmt.Sprintf("%s://%s/disknotify?name=%s&md5=%s&size=%d", config.Protocol(), config.Receiver_Address, dest_path, stream_md5.SumString(), bin_part.TotalSize)
	request, req_err := http.NewRequest("POST", post_url, nil)
	request.Header.Add("X-STS-SenderName", config.Sender_Name)
	if req_err != nil {
		util.LogError("Could not generate HTTP request object: ", req_err.Error())
	}
	resp, req_err := sender.client.Do(request)
	if req_err != nil {
		util.LogError("Encountered", req_err.Error(), "while trying to send", dest_path, "confirmation request")
	}
	resp_bytes, read_err := ioutil.ReadAll(resp.Body)
	if read_err != nil {
		util.LogError("Unable to read server confirmation of disk write:", read_err.Error())
	}
	resp.Body.Close()
	resp.Close = true
	if string(resp_bytes) == "200" {
		send_bin.delete() // Write finished, delete bin.
	} else {
		util.LogError("Unexpected response in disk confirmation request:", string(resp_bytes))
		sender.passBackBin(send_bin)
	}
}

// sendGridFTP accepts Bins with transfer type GridFTP, and does nothing with them yet.
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

// NewBinBody creates a new instance of a BinBody from an instance of Bin.
// It takes the optional boundary argument, which sets the multipart writer boundary upon creation.
func NewBinBody(bin Bin, boundary ...string) *BinBody {
	if len(bin.Files) < 1 {
		util.LogError("Tried to convert empty Bin to bytes")
		panic("Fatal error")
	}
	new_body := &BinBody{}
	new_body.eof_returned = false
	new_body.compression = false
	new_body.gzip_buffer = bytes.Buffer{}
	var gzip_creation_err error
	new_body.gzip_writer, gzip_creation_err = gzip.NewWriterLevel(&new_body.gzip_buffer, gzip.BestCompression)
	if gzip_creation_err != nil {
		util.LogError("Could not create gzip writer:", gzip_creation_err.Error())
	}
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
	var open_err error
	body.file_handle, open_err = os.Open(body.bin_part.Path)
	if open_err != nil {
		util.LogError(fmt.Sprintf("Could not open file %s while creating Bin body: %s", body.bin_part.Path, open_err.Error()))
	}
	body.file_handle.Seek(body.bin_part.Start, 0)
	new_header := textproto.MIMEHeader{}
	new_header.Add("Content-Disposition", "form-data")
	new_header.Add("Content-Type", "application/octet-stream")
	new_header.Add("X-STS-FileName", getStorePath(body.bin_part.Path, body.bin.WatchDir))
	new_header.Add("X-STS-FileSize", fmt.Sprintf("%d", body.bin_part.TotalSize))
	new_header.Add("X-STS-PartLocation", getPartLocation(body.bin_part.Start, body.bin_part.End))
	new_header.Add("X-STS-PartHash", body.bin_part.MD5)
	new_header.Add("X-STS-FileHash", body.bin_part.File_MD5)
	new_header.Add("X-STS-PrevFileName", body.bin_part.Last_File)
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
