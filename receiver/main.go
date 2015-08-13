package main

import (
    "compress/gzip"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "mime/multipart"
    "net/http"
    "net/textproto"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "time"
    "util"
)

// companion_lock prevents the same companion file from being written to by multiple threads.
// This is done in order to avoid overwriting new part data in companion files.
var companion_lock bool

// errorHandler is called when any page that is not a registered API method is requested.
func errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}

// getChunkLocation parses the "location" file Header and returns the first and last byte from the chunk.
func getChunkLocation(location string) (int64, int64) {
    split_chunk := strings.Split(location, ":")
    start, _ := strconv.ParseInt(split_chunk[0], 10, 64)
    end, _ := strconv.ParseInt(split_chunk[1], 10, 64)
    return start, end
}

// sendHandler receives a multipart file from the sender via PUT request.
// It is responsible for verifying the md5 of each chunk in the file, replicating it's directory structure as it was on the sender, and writing the file to disk.
func sendHandler(w http.ResponseWriter, r *http.Request) {
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
        handlePart(next_part, boundary, compression)
    }
    fmt.Fprint(w, http.StatusOK)
}

// handlePart is called for each part in the multipart file that is sent to sendHandler.
// handlePart reads from the Part stream, and writes the part to a file while calculating the md5.
// If the file is bad, it will be reaquired and passed back into sendHandler.
func handlePart(part *multipart.Part, boundary string, compressed bool) {
    // Gather data about part from headers
    part_path := part.Header.Get("name")
    write_path := part_path + ".tmp"
    part_start, part_end := getChunkLocation(part.Header.Get("location"))
    part_md5 := util.NewStreamMD5()
    // If the file which this chunk belongs to does not already exist, create a new empty file and companion file.
    _, err := os.Open(write_path)
    if os.IsNotExist(err) {
        total_size, _ := strconv.ParseInt(part.Header.Get("total_size"), 10, 64)
        fmt.Println(total_size)
        createEmptyFile(part_path, total_size)
    }
    // Start reading and iterating over part
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
        if compressed {
            bytes_read, err = gzip_reader.Read(part_bytes)
        } else {
            bytes_read, err = part.Read(part_bytes) // This doesn't always return BlockSize
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
        // Validation failed, request this specific chunk again using chunk size
        fmt.Printf("Bad chunk of %s from bytes %s", part_path, part.Header.Get("location"))
        new_stream := requestPart(part_path, part.Header, part_start, part_end, boundary)
        time.Sleep(5 * time.Second)
        handlePart(new_stream, boundary, compressed)
        return
    }
    // Update the companion file of the part, and check if the whole file is done
    addPartToCompanion(part_path, part.Header.Get("md5"), part.Header.Get("location"))
    if isFileComplete(part_path) {
        fmt.Println("Fully assembled ", part_path)
        os.Rename(write_path, part_path)
        os.Remove(part_path + ".comp")
    }
}

// removeFromCache removes the specified file from the cache on the Sender.
// removeFromCache recursively calls itself until the Sender confirms that the file has been removed.
func removeFromCache(path string) {
    client := http.Client{}
    post_url := fmt.Sprintf("http://localhost:8080/remove.go?name=%s", path)
    request, _ := http.NewRequest("POST", post_url, nil)
    _, err := client.Do(request)
    if err != nil {
        fmt.Println("Request to remove from cache failed")
        time.Sleep(5 * time.Second)
        removeFromCache(path)
    }
}

// isFileComplete decodes the companion file of a given path and determines whether the file is complete.
// isFileComplete sums the number of bytes in each chunk in the companion file. If the sum equals the total file size, the file is marked as complete.
func isFileComplete(path string) bool {
    is_done := false
    decoded_companion := decodeCompanion(path)
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

// Companion is a struct that represents the data of a JSON companion file.
type Companion struct {
    Path         string
    TotalSize    int64
    CurrentParts []string
}

// decodeCompanion takes the path of the "final file", decodes, and returns the companion struct that can be found at that path.
func decodeCompanion(path string) *Companion {
    path = path + ".comp"
    new_companion := &Companion{}
    companion_bytes, _ := ioutil.ReadFile(path)
    json.Unmarshal(companion_bytes, new_companion)
    return new_companion
}

// addPartToCompanion decodes a companion struct, adds the specified id to CurrentParts (must be unique) and writes the modified companion struct back to disk.
// It implements a "companion lock" system, to prevent the same companion file being written to by two goroutines at the same time.
// addPartToCompanion takes the path of the "final file".
func addPartToCompanion(path string, id string, location string) {
    if !companion_lock {
        companion_lock = true
    } else {
        time.Sleep(20 * time.Millisecond)
        addPartToCompanion(path, id, location)
        return
    }
    companion := decodeCompanion(path)
    chunk_addition := id + ";" + location
    if !util.IsStringInArray(companion.CurrentParts, chunk_addition) {
        companion.CurrentParts = append(companion.CurrentParts, chunk_addition)
    }
    companion.encodeAndWrite()
    companion_lock = false
}

// encodeAndWrite takes the in-memory representation of a companion file, creates a JSON representation, and writes it to disk.
func (comp *Companion) encodeAndWrite() {
    companion_bytes, _ := json.Marshal(comp)
    comp_file, _ := os.OpenFile(comp.Path+".comp.tmp", os.O_RDWR|os.O_CREATE, 0700)
    comp_file.Write(companion_bytes)
    comp_file.Close()
    os.Rename(comp.Path+".comp.tmp", comp.Path+".comp")
}

// newCompanion creates a new companion file initialized with specified parameters, and writes it to disk.
func newCompanion(path string, size int64) {
    current_parts := make([]string, 0)
    new_companion := Companion{path, size, current_parts}
    new_companion.encodeAndWrite()
}

// requestPart sends an HTTP request to the sender which requests a file part.
// After receiving a file part, requestPart will create a multipart file with only one chunk, and pass it back into handleFile for validation.
func requestPart(path string, part_header textproto.MIMEHeader, start int64, end int64, boundary string) *multipart.Part {
    post_url := fmt.Sprintf("http://localhost:8080/get_file.go?name=%s&start=%d&end=%d", path, start, end)
    client := http.Client{}
    request, _ := http.NewRequest("POST", post_url, nil)
    resp, req_err := client.Do(request)
    if req_err != nil {
        fmt.Println("Failed to re-request part. Sender must be down.")
        time.Sleep(5 * time.Second)
        return requestPart(path, part_header, start, end, boundary) // If the sender is down for a really really long time, could cause a stack overflow
    }
    reader := multipart.NewReader(resp.Body, boundary)
    part, part_err := reader.NextPart()
    if part_err != nil {
        fmt.Println(part_err)
    }
    return part
}

// createNewFile is called when a multipart chunk is encountered and the whole file hasn't been created on the reciever yet.
// It fills a file with zero value bytes so that the created file is equal to the final size of multipart file.
func createEmptyFile(path string, size int64) {
    os.MkdirAll(filepath.Dir(path), os.ModePerm)
    newCompanion(path, size)
    fi, _ := os.Create(path + ".tmp")
    fi.Truncate(size)
    fi.Close()
}

func finishFile(addition_channel chan string, config_file string) {
    for {
        new_file := <-addition_channel
        if !(strings.HasSuffix(new_file, ".comp") || strings.HasSuffix(new_file, ".tmp")) && new_file != config_file {
            removeFromCache(new_file)
            fmt.Println("Removed ", new_file, " from cache")
        }
    }
}

// main is the entry point of the webserver. It is responsible for registering handlers and beginning the request serving loop.
func main() {
    companion_lock = false
    // Create and start listener
    cwd, _ := os.Getwd()
    addition_channel := make(chan string, 1)
    listener_cache_file := "listener_cache.dat"
    listener_cache_file, _ = filepath.Abs(listener_cache_file)
    listener := util.ListenerFactory("listener_cache.dat", cwd)
    go finishFile(addition_channel, listener_cache_file)
    listener.LoadCache()
    go listener.Listen(addition_channel)

    http.HandleFunc("/send.go", sendHandler)
    http.HandleFunc("/", errorHandler)
    http.ListenAndServe(":8081", nil)
}
