package main

import (
    "compress/gzip"
    "fmt"
    "mime/multipart"
    "net"
    "net/http"
    "net/textproto"
    "os"
    "path/filepath"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "time"
    "util"
)

const FINAL_DIRECTORY = "final"

// finalize_mutex prevents the cache from updating its timestamp while files
// from its addition channel are being processed.
var finalize_mutex sync.Mutex

// errorHandler is called when any page that is not a registered API method is requested.
func errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}

// getPartLocation parses the "location" part Header and returns the first and last byte from the part.
// Location format: "start_byte:end_byte"
func getPartLocation(location string) (int64, int64) {
    split_header := strings.Split(location, ":")
    start, _ := strconv.ParseInt(split_header[0], 10, 64)
    end, _ := strconv.ParseInt(split_header[1], 10, 64)
    return start, end
}

// sendHandler receives a multipart file from the sender via PUT request.
// It is responsible for verifying the md5 of each part in the file, replicating
// its directory structure as it was on the sender, and writing the file to disk.
func sendHandler(w http.ResponseWriter, r *http.Request) {
    ip, _, _ := net.SplitHostPort(r.RemoteAddr)
    host_name := util.GetHostname(ip)
    compression := false
    if r.Header.Get("Content-Encoding") == "gzip" {
        compression = true
    }
    boundary := r.Header.Get("Boundary")
    multipart_reader := multipart.NewReader(r.Body, boundary)
    for {
        next_part, end_of_file := multipart_reader.NextPart()
        if end_of_file != nil { // Reached end of multipart file
            break
        }
        handlePart(next_part, boundary, host_name, compression)
    }
    fmt.Fprint(w, http.StatusOK)
}

// handlePart is called for each part in the multipart file that is sent to sendHandler.
// It reads from the Part stream and writes the part to a file while calculating the md5.
// If the file is bad, it will be reacquired and passed back into sendHandler.
func handlePart(part *multipart.Part, boundary string, host_name string, compressed bool) {
    // Gather data about part from headers
    part_path := part.Header.Get("name")
    write_path := part_path + ".tmp"
    part_start, part_end := getPartLocation(part.Header.Get("location"))
    part_md5 := util.NewStreamMD5()
    // If the file which this part belongs to does not already exist, create a new empty file and companion file.
    _, err := os.Open(write_path)
    if os.IsNotExist(err) {
        total_size, _ := strconv.ParseInt(part.Header.Get("total_size"), 10, 64)
        createEmptyFile(part_path, total_size)
    }
    // Start reading and iterating over the part
    part_fi, _ := os.OpenFile(write_path, os.O_WRONLY, 0600)
    part_fi.Seek(part_start, 0)
    part_bytes := make([]byte, part_md5.BlockSize)
    var gzip_reader *gzip.Reader
    if compressed {
        gzip_reader, _ = gzip.NewReader(part)
    }
    for {
        var err error
        var bytes_read int
        // The number of bytes read can often be less than the size of the passed buffer.
        if compressed {
            bytes_read, err = gzip_reader.Read(part_bytes)
        } else {
            bytes_read, err = part.Read(part_bytes)
        }
        part_md5.Update(part_bytes[0:bytes_read])
        part_fi.Write(part_bytes[0:bytes_read])
        if err != nil {
            break
        }
    }
    part_fi.Close()
    // Validate part
    if part.Header.Get("md5") != part_md5.SumString() {
        // Validation failed, request this specific part again using part size
        fmt.Printf("Bad part of %s from bytes %s", part_path, part.Header.Get("location"))
        new_stream := requestPart(part_path, part.Header, part_start, part_end, boundary)
        time.Sleep(5 * time.Second)
        handlePart(new_stream, boundary, host_name, compressed)
        return
    }
    // Update the companion file of the part, and check if the whole file is done
    util.AddPartToCompanion(part_path, part.Header.Get("md5"), part.Header.Get("location"))
    if isFileComplete(part_path) {
        // Finish file by moving it to the final directory and removing the .tmp extension.
        final_path := util.JoinPath(FINAL_DIRECTORY, host_name, part_path)
        os.MkdirAll(filepath.Dir(final_path), os.ModePerm) // Make containing directories for the file.
        rename_err := os.Rename(write_path, final_path)
        if rename_err != nil {
            panic(rename_err.Error())
        }
        os.Chtimes(part_path, time.Now(), time.Now()) // Update mtime so that listener will pick up the file
        os.Remove(part_path + ".comp")                // Delete the companion file.
        fmt.Println("Fully assembled ", part_path)
    }
}

// removeFromCache removes the specified file from the cache on the Sender.
// It loops until the Sender confirms that the file has been removed.
func removeFromCache(path string) {
    request_complete := false
    client := http.Client{}
    post_url := fmt.Sprintf("http://localhost:8080/remove.go?name=%s", path)
    for !request_complete {
        request, _ := http.NewRequest("POST", post_url, nil)
        _, err := client.Do(request)
        if err != nil {
            fmt.Println("Request to remove from cache failed")
            time.Sleep(5 * time.Second)
        } else {
            request_complete = true
        }
    }
}

// isFileComplete decodes the companion file of a given path and determines whether the file is complete.
// It sums the number of bytes in each part in the companion file. If the sum equals the total file size,
// the function returns true.
func isFileComplete(path string) bool {
    is_done := false
    decoded_companion := util.DecodeCompanion(path)
    companion_size := int64(0)
    for _, element := range decoded_companion.CurrentParts {
        part_locations := strings.Split(strings.Split(element, ";")[1], ":")
        start, _ := strconv.ParseInt(part_locations[0], 10, 64)
        end, _ := strconv.ParseInt(part_locations[1], 10, 64)
        part_size := end - start
        companion_size += part_size
    }
    if companion_size == decoded_companion.TotalSize {
        is_done = true
    }
    return is_done
}

// requestPart sends an HTTP request to the sender's get_file API. The sender will
// reply with the body of a multipart file with the path, start, end, and boundary that
// is specified. After receiving a file part with associated header data, requestPart will
// read out the first and only part, and pass it back into handleFile for validation.
func requestPart(path string, part_header textproto.MIMEHeader, start int64, end int64, boundary string) *multipart.Part {
    post_url := fmt.Sprintf("http://localhost:8080/get_file.go?name=%s&start=%d&end=%d&boundary=%s", path, start, end, boundary)
    client := http.Client{}
    request_complete := false
    var return_part *multipart.Part
    for !request_complete {
        request, _ := http.NewRequest("POST", post_url, nil)
        resp, req_err := client.Do(request)
        if req_err != nil {
            fmt.Println("Failed to re-request part. Sender must be down.")
            time.Sleep(5 * time.Second)
            continue
        }
        reader := multipart.NewReader(resp.Body, boundary)
        part, part_err := reader.NextPart()
        if part_err != nil {
            fmt.Println(part_err.Error())
            continue
        }
        return_part = part
        request_complete = true
    }
    return return_part
}

// createNewFile is called when a multipart part is encountered and the file doesn't exist on the receiver yet.
// It fills a file with null bytes so that the created file is the same size as the complete file will be.
func createEmptyFile(path string, size int64) {
    os.MkdirAll(filepath.Dir(path), os.ModePerm)
    util.NewCompanion(path, size)
    fi, _ := os.Create(path + ".tmp")
    fi.Truncate(size)
    fi.Close()
}

// getStorePath returns the path that the receiver should use to store a file.
// Given parameters full_path and watch_directory, it will remove watch directory from the full path.
// This function differs from getStorePath() on the sender because the receiver watches its containing directory.
func getStorePath(full_path string, watch_directory string) string {
    store_path := strings.Replace(full_path, watch_directory+string(os.PathSeparator), "", 1)
    return store_path
}

// finishFile blocks while listening for any additions on addition_channel.
// Once a file that isn't a temp file is found, it removes the file from the sender's cache.
func finishFile(addition_channel chan string) {
    for {
        new_file := <-addition_channel
        // Acquire the mutex while working with new files so that the cache will re-detect unprocessed files in the event of a crash.
        finalize_mutex.Lock()
        func() {
            defer finalize_mutex.Unlock()
            cwd, _ := os.Getwd()
            removeFromCache(getStorePath(new_file, cwd))
        }()
    }
}

// onFinish will prevent the cache from writing newer timestamps to disk while
// files from the addition channel are still being processed by finishFile.
// In the event of a crash, the listener will pick up unfinished files again.
func onFinish() {
    finalize_mutex.Lock()
    finalize_mutex.Unlock()
}

// main is the entry point of the webserver. It is responsible for registering
// handlers and beginning the request serving loop. It also creates and starts the file listener.
func main() {
    finalize_mutex = sync.Mutex{}
    util.CompanionLock = sync.Mutex{}
    // Setup listener and add ignore patterns.
    cwd, _ := os.Getwd()
    addition_channel := make(chan string, 1)
    listener_cache_file := "listener_cache.dat"
    listener := util.NewListener(listener_cache_file, cwd)
    listener.SetOnFinish(onFinish)
    listener.AddIgnored(`\.tmp`)
    listener.AddIgnored(`\.comp`)
    listener.AddIgnored(regexp.QuoteMeta(listener_cache_file))
    go finishFile(addition_channel)
    listener.LoadCache()
    go listener.Listen(addition_channel)
    // Register request handling functions
    http.HandleFunc("/send.go", sendHandler)
    http.HandleFunc("/", errorHandler)
    http.ListenAndServe(":8081", nil)
}
