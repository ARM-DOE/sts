package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
)

// Webserver acts as a layer of abstraction to separate webserver functions from the "main" functions in the rest of the package.
type Webserver struct {
}

// WebserverFactory creates and returns a new Webserver struct
func WebserverFactory() *Webserver {
    return &Webserver{}
}

// startServer is the entry point of the webserver. It is responsible for registering handlers and beginning the request serving loop.
func (server *Webserver) startServer() {
    http.HandleFunc("/get_file.go", server.getFile)
    http.HandleFunc("/", server.errorHandler)
    http.ListenAndServe(":8080", nil)
}

// getFile takes a file name and amount of bytes already read to return a whole or partial file.
// This handler is called when "get_file.go" is requested.
// Sample request: /get_file.go?name=watch_directory/test_file.txt&bytes=17
func (server *Webserver) getFile(w http.ResponseWriter, r *http.Request) {
    name := r.FormValue("name")
    string_bytes := r.FormValue("bytes")
    bytes_already_sent, parse_err := strconv.ParseInt(string_bytes, 10, 64)
    if parse_err != nil {
        fmt.Fprint(w, "byte argument "+string_bytes+" invalid")
        return
    }
    fi, err := os.Open(name)
    if err != nil {
        fmt.Fprint(w, "File "+name+" not found")
    } else {
        file_info, _ := fi.Stat()
        unsent_byte_count := file_info.Size() - bytes_already_sent
        if unsent_byte_count < 0 {
            fmt.Fprint(w, "byte count less than 0")
            return
        }
        bytes_to_send := make([]byte, unsent_byte_count)
        fi.Seek(bytes_already_sent, 0)
        io.ReadAtLeast(fi, bytes_to_send, int(unsent_byte_count))
        w.Write(bytes_to_send)
    }
}

// errorHandler is called when any page that is not a registered API method is requested.
func (server *Webserver) errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}
