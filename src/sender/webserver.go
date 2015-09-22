package sender

import (
    "fmt"
    "net"
    "net/http"
    "os"
    "strconv"
    "util"
)

// Webserver acts as a layer of abstraction to separate webserver functions
// from the "main" functions in the rest of the package.
// It also contains some config data that the webserver needs during runtime.
type Webserver struct {
    cache *Cache // Instance of the cache, used to remove entries from the cache and access config values.
}

// NewWebserver creates and returns a new Webserver struct
// cache is a pointer to the cache, used for cache entry
// removals over HTTP and obtaining config values.
func NewWebserver(cache *Cache) *Webserver {
    new_server := &Webserver{}
    new_server.cache = cache
    return new_server
}

// startServer is the entry point of the webserver. It is responsible for registering
// handlers and beginning the request serving loop.
func (server *Webserver) startServer() net.Listener {
    http.HandleFunc("/get_file.go", server.getFile)
    http.HandleFunc("/remove.go", server.removeFromCache)
    http.HandleFunc("/", server.errorHandler)
    http.HandleFunc("/edit_config.go", config.EditConfig)
    http.HandleFunc("/editor.go", config.EditConfigInterface)
    serv, serv_err := util.AsyncListenAndServeTLS(fmt.Sprintf(":%s", config.Server_Port), config.Server_SSL_Cert, config.Server_SSL_Key)
    if serv_err != nil {
        error_log.LogError(serv_err.Error())
    }
    return serv
}

// getFile takes a file name, start, and end position in the file to return a whole or partial file
// as a multipart request. This handler is called when "get_file.go" is requested.
// Sample request: /get_file.go?name=watch_directory/test_file.txt&start=0&end=30&boundary=865876njgbhrnghu
func (server *Webserver) getFile(w http.ResponseWriter, r *http.Request) {
    name := r.FormValue("name")
    boundary := r.FormValue("boundary")
    start_byte_string := r.FormValue("start")
    end_byte_string := r.FormValue("end")
    start_byte, start_err := strconv.ParseInt(start_byte_string, 10, 64)
    end_byte, end_err := strconv.ParseInt(end_byte_string, 10, 64)
    if start_err != nil || end_err != nil {
        fmt.Fprint(w, http.StatusBadRequest)
        return
    }
    fi, err := os.Open(name)
    file_info, _ := fi.Stat()
    if err != nil {
        fmt.Fprintln(w, http.StatusNotFound)
        return
    } else if end_byte > file_info.Size() || start_byte > end_byte {
        fmt.Fprint(w, http.StatusBadRequest)
        return
    } else {
        bin := NewBin(server.cache, server.cache.BinSize(), server.cache.listener.WatchDir(0))
        bin.addPart(name, start_byte, end_byte, file_info)
        md5, md5_err := bin.Files[0].getMD5()
        bin.Files[0].MD5 = md5
        if md5_err != nil {
            error_log.LogError(md5_err.Error())
        }
        bin_stream := NewBinBody(bin, boundary)
        byte_buffer := make([]byte, util.DEFAULT_BLOCK_SIZE)
        for {
            bytes_read, eof := bin_stream.Read(byte_buffer)
            w.Write(byte_buffer[0:bytes_read])
            if eof != nil {
                break
            }
        }
    }
}

// removeFromCache is called when the receiver confirms that a file is totally
// sent, and wants the Sender to remove it from the list of files that are in line to be processed.
// It takes only a file name parameter via POST request.
// Example request: /remove.go?name=watch_directory/test_file.txt
func (server *Webserver) removeFromCache(w http.ResponseWriter, r *http.Request) {
    file_path := r.FormValue("name")
    file_path = getWholePath(file_path)
    file_tag := getTag(file_path)
    server.cache.removeFile(r.FormValue("name"))
    if file_tag.Delete_On_Send {
        os.Remove(file_path)
    }
    fmt.Fprint(w, http.StatusOK)
}

// errorHandler is called when any page that is not a registered API method is requested.
func (server *Webserver) errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}
