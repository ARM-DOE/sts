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

// errorHandler is called when any page that is not a registered API method is requested.
func errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}

// handler receives a file from the sender via PUT request, with an added HTTP header with key "metadata" that describes the file.
// Format of "metadata": {"file": {"path": file_path_on_sender, "md5": file_md5, "size": file_size_in_bytes, "modtime": modtime_with_timezone, "store_path": file_path_to_store_at}}
func handler(w http.ResponseWriter, r *http.Request) {
    byte_buffer := new(bytes.Buffer)
    byte_buffer.ReadFrom(r.Body)
    go handleFile(byte_buffer, r.Header.Get("metadata"))
}

// handleFile is called by handler when a file and the metadata are sent via PUT request.
// It is responsible for verifying the md5 of the file, replicating it's directory structure as it was on the sender, and writing the file to disk.
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

// decodeJSON is a temporary function to split the values of the JSON file metadata string into a dictionary.
func decodeJSON(json string) map[string]string {
    decoded_json := make(map[string]string)
    json_key_order := [5]string{"path", "md5", "size", "modtime", "store_path"}
    pattern := regexp.MustCompile(`(": ")(.+?")`)
    for index, element := range pattern.FindAllString(json, 50) {
        tmp := strings.Replace(element, `"`, "", 3)
        decoded_json[json_key_order[index]] = strings.Replace(tmp, ": ", "", 1)
    }
    return decoded_json
}

// main is the entry point of the webserver. It is responsible for registering handlers and beginning the request serving loop.
func main() {
    http.HandleFunc("/send.go", handler)
    http.HandleFunc("/", errorHandler)
    http.ListenAndServe(":8081", nil)
}
