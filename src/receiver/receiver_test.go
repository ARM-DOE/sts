package receiver

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"testing"
	"util"
)

// Send a multipart file with multiple parts to the receiver
func TestMultiPartReceive(t *testing.T) {
	// Start webserver
	go Main("../../conf/receiver_config.yaml")
	// Create multipart file
	defer os.Remove(util.JoinPath("out", "receiver", "final", "localhost", "test_dir", "small_file.txt"))
	defer os.Remove(util.JoinPath("out", "receiver", "final", "localhost", "test_dir", "other_file.txt"))
	defer os.Remove(util.JoinPath("stage", "test_dir", "small_file.txt.comp"))
	defer os.Remove(util.JoinPath("stage", "test_dir", "other_file.txt.comp"))
	buff := bytes.Buffer{}
	small_file := []byte("a small file of only one chunk")
	writer := multipart.NewWriter(&buff)
	header := textproto.MIMEHeader{}
	header.Add("Content-Disposition", "form-data")
	header.Add("Content-Type", "application/octet-stream")
	header.Add("md5", "015f68da525e180351a3be524ff44d61")
	header.Add("name", "test_dir/small_file.txt")
	header.Add("total_size", fmt.Sprintf("%d", len(small_file)))
	header.Add("location", fmt.Sprintf("%d:%d", 0, len(small_file)))
	new_part, _ := writer.CreatePart(header)
	new_part.Write(small_file)
	header.Set("name", "test_dir/other_file.txt")
	second_part, _ := writer.CreatePart(header)
	second_part.Write(small_file)
	writer.Close()
	request, _ := http.NewRequest("PUT", "https://localhost:8081/send.go", bytes.NewReader(buff.Bytes()))
	request.Header.Add("Transfer-Encoding", "chunked")
	request.Header.Add("Boundary", writer.Boundary())
	client, client_err := util.GetTLSClient(config.Client_SSL_Cert, config.Client_SSL_Key)
	if client_err != nil {
		t.Error(client_err.Error())
	}
	client.Do(request)
	// Check that files exist
	fi, open_err := os.Open(util.JoinPath("final", "localhost", "test_dir", "small_file.txt"))
	if open_err != nil {
		t.Error("Sent file not created")
	}
	read_bytes, _ := ioutil.ReadAll(fi)
	if string(read_bytes) != string(small_file) {
		t.Error("Contents of first file not the same as sent data")
	}
	fi2, open_err2 := os.Open(util.JoinPath("final", "localhost", "test_dir", "other_file.txt"))
	if open_err2 != nil {
		t.Error("Sent file not created")
	}
	read_bytes2, _ := ioutil.ReadAll(fi2)
	if string(read_bytes2) != string(small_file) {
		t.Error("Contents of file not the same as sent data")
	}
}

// Send a multipart file with only one part to the receiver
func TestSinglePartReceive(t *testing.T) {
	// Create multipart file
	defer os.Remove(util.JoinPath("final", "localhost", "test_dir", "small_file.txt"))
	defer os.Remove(util.JoinPath("stage", "test_dir", "small_file.txt.comp"))
	buff := bytes.Buffer{}
	small_file := []byte("a small file of only one chunk")
	writer := multipart.NewWriter(&buff)
	header := textproto.MIMEHeader{}
	header.Add("Content-Disposition", "form-data")
	header.Add("Content-Type", "application/octet-stream")
	header.Add("md5", "015f68da525e180351a3be524ff44d61")
	header.Add("name", "test_dir/small_file.txt")
	header.Add("total_size", fmt.Sprintf("%d", len(small_file)))
	header.Add("location", fmt.Sprintf("%d:%d", 0, len(small_file)))
	new_part, _ := writer.CreatePart(header)
	new_part.Write(small_file)
	writer.Close()
	request, _ := http.NewRequest("PUT", "https://localhost:8081/send.go", bytes.NewReader(buff.Bytes()))
	request.Header.Add("Transfer-Encoding", "chunked")
	request.Header.Add("Boundary", writer.Boundary())
	client, client_err := util.GetTLSClient("../../conf/client.pem", "../../conf/client.key")
	if client_err != nil {
		t.Error(client_err)
		return
	}
	client.Do(request)
	// Check that file exists
	fi, open_err := os.Open(util.JoinPath("final", "localhost", "test_dir", "small_file.txt"))
	if open_err != nil {
		t.Error("Sent file not created")
	}
	read_bytes, _ := ioutil.ReadAll(fi)
	if string(read_bytes) != string(small_file) {
		t.Error("Contents of file not the same as sent data")
	}
}

// Cleans garbage files from src after test completion
func TestCleanup(t *testing.T) {
	os.RemoveAll("final")
	os.RemoveAll("stage")
	os.Remove("listener_cache.dat")
}
