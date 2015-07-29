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

// getFile takes a file name and a start and end position in the file to return a whole or partial file.
// This handler is called when "get_file.go" is requested.
// Sample request: /get_file.go?name=watch_directory/test_file.txt&start=0&end=30
func (server *Webserver) getFile(w http.ResponseWriter, r *http.Request) {
    name := r.FormValue("name")
    start_byte_string := r.FormValue("start")
    end_byte_string := r.FormValue("end")
    start_byte, start_err := strconv.ParseInt(start_byte_string, 10, 64)
    end_byte, end_err := strconv.ParseInt(end_byte_string, 10, 64)
    if start_err != nil || end_err != nil {
        fmt.Fprint(w, "byte argument invalid")
        return
    }
    fi, err := os.Open(name)
    if err != nil {
        fmt.Fprint(w, "File "+name+" not found")
    } else {
        file_info, _ := fi.Stat()
        if end_byte > file_info.Size() || start_byte > end_byte {
            fmt.Fprint(w, "could not retrieve specified file portion")
        }
        byte_count := end_byte - start_byte
        bytes_to_send := make([]byte, byte_count)
        fi.Seek(start_byte, 0)
        io.ReadAtLeast(fi, bytes_to_send, int(byte_count))
        w.Write(bytes_to_send)
    }
}

// errorHandler is called when any page that is not a registered API method is requested.
func (server *Webserver) errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}
