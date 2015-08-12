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
    cache     *Cache
    watch_dir string
}

// WebserverFactory creates and returns a new Webserver struct
func WebserverFactory(cache *Cache, watch_dir string) *Webserver {
    new_server := &Webserver{}
    new_server.cache = cache
    new_server.watch_dir = watch_dir
    return new_server
}

// startServer is the entry point of the webserver. It is responsible for registering handlers and beginning the request serving loop.
func (server *Webserver) startServer() {
    http.HandleFunc("/get_file.go", server.getFile)
    http.HandleFunc("/remove.go", server.removeFromCache)
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
            fmt.Fprint(w, http.StatusBadRequest)
        }
        byte_count := end_byte - start_byte
        bytes_to_send := make([]byte, byte_count)
        fi.Seek(start_byte, 0)
        io.ReadAtLeast(fi, bytes_to_send, int(byte_count))
        w.Write(bytes_to_send)
    }
}

// removeFromCache is called when the receiver confirms that a file is totally sent, and wants the Sender to remove it from the list of files that are in line to be processed.
// removeFromCache takes only a file name argument.
// Example request: /remove.go?name=watch_directory/test_file.txt
func (server *Webserver) removeFromCache(w http.ResponseWriter, r *http.Request) {
    file_path := r.FormValue("name")
    file_path = getWholePath(file_path)
    server.cache.removeFile(file_path)
}

// errorHandler is called when any page that is not a registered API method is requested.
func (server *Webserver) errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}
