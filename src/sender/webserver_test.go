package sender

import (
    "fmt"
    "io/ioutil"
    "mime/multipart"
    "net/http"
    "os"
    "testing"
    "util"
)

var client http.Client

// TestServer starts a new instance of Webserver and checks to see that the error page is serving correctly.
func TestServer(t *testing.T) {
    config = util.Config{}
    config.Server_Port = "8080"
    config.Sender_Server_Port = "8080"
    config.Receiver_Address = "localhost"
    config.Client_SSL_Cert = "../conf/client.pem"
    config.Client_SSL_Key = "../conf/client.key"
    config.Server_SSL_Cert = "../conf/server.pem"
    config.Server_SSL_Key = "../conf/server.key"
    client, _ = util.GetTLSClient(config.Client_SSL_Cert, config.Client_SSL_Key)
    dummy_bin_chan := make(chan Bin)
    cwd, _ := os.Getwd()
    dummy_cache := NewCache("/dev/null", cwd, 3000, dummy_bin_chan)
    server := NewWebserver(dummy_cache)
    server.startServer()
    request, err := http.NewRequest("GET", "https://localhost:8080/not_a_real_page.go", nil)
    if err != nil {
        t.Error(err.Error())
        return
    }
    resp, send_err := client.Do(request)
    if send_err != nil {
        t.Error(send_err.Error())
        return
    }
    resp_content, read_err := ioutil.ReadAll(resp.Body)
    if read_err != nil {
        t.Error(read_err)
        return
    }
    if string(resp_content) != "404" {
        t.Error("Non-existent page did not return 404")
    }
}

// TestGetFile checks to see whether the webserver will return a valid multipart request from get_file.go
func TestGetFile(t *testing.T) {
    fi, _ := os.Open("send_test.txt")
    file_content, _ := ioutil.ReadAll(fi)
    url := fmt.Sprintf("https://localhost:8080/get_file.go?name=send_test.txt&start=0&end=%d&boundary=12254eb56f10eb966eb96d6e108e9a98e1a16949aca7f4939666ada18c40", len(file_content))
    request, _ := http.NewRequest("POST", url, nil)
    response, err := client.Do(request)
    if err != nil {
        t.Error(err.Error())
    }
    reader := multipart.NewReader(response.Body, "12254eb56f10eb966eb96d6e108e9a98e1a16949aca7f4939666ada18c40")
    first_part, err := reader.NextPart()
    if err != nil {
        t.Error("No part sent, or part is not parsable")
        t.Error(err.Error())
        return
    }
    part_content, _ := ioutil.ReadAll(first_part)
    if string(part_content) != string(file_content) {
        t.Error("Receiver file did not match sent file")
    }
}
