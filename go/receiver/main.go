package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func errorHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Page does not exist")
}

func handler(w http.ResponseWriter, r *http.Request) {
	byte_buffer := new(bytes.Buffer)
	byte_buffer.ReadFrom(r.Body)
	handleFile(byte_buffer, r.Header.Get("metadata"))
}

func handleFile(bytes_of_file *bytes.Buffer, metadata string) {
	// Generate md5 of recieved file
	md5_to_verify := md5.New()
	io.WriteString(md5_to_verify, bytes_of_file.String())
	md5_bytes := md5_to_verify.Sum(nil)
	md5_string := fmt.Sprintf("%x", md5_bytes)

	decoded_metadata := decodeJSON(metadata)
	if !(decoded_metadata["md5"] == md5_string) {
		// File does not match md5
		fmt.Println("Bad file " + decoded_metadata["path"])
		return
	}
	os.MkdirAll(filepath.Dir(decoded_metadata["store_path"]), os.ModePerm)
	err := ioutil.WriteFile(decoded_metadata["store_path"], bytes_of_file.Bytes(), 0700)
	if err != nil {
		fmt.Println("Error writing " + decoded_metadata["store_path"] + " to disk - " + err.Error())
	} else {
		fmt.Println("Wrote " + decoded_metadata["store_path"] + " to disk")
	}
}

func decodeJSON(json string) map[string]string {
	// Only use for metadata header
	decoded_json := make(map[string]string)
	json_key_order := [5]string{"path", "md5", "size", "modtime", "store_path"}
	pattern := regexp.MustCompile(`(": ")(.+?")`)
	for index, element := range pattern.FindAllString(json, 50) {
		tmp := strings.Replace(element, `"`, "", 3)
		decoded_json[json_key_order[index]] = strings.Replace(tmp, ": ", "", 1)
	}
	return decoded_json
}

func main() {
	http.HandleFunc("/send.go", handler)
	http.HandleFunc("/", errorHandler)
	http.ListenAndServe(":8080", nil)
}
