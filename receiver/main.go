package main

import (
    "bytes"
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

// handler receives a multipart file from the sender via PUT request, with an added HTTP header that specifies the boundary used to split the multipart file.
// handler calls handleFile as a goroutine, which validates and writes the file to disk.
func handler(w http.ResponseWriter, r *http.Request) {
    byte_buffer := bytes.Buffer{}
    byte_buffer.ReadFrom(r.Body)
    handleFile(byte_buffer.Bytes())
}

// getChunkLocation parses the "location" file Header and returns the first and last byte from the chunk.
func getChunkLocation(location string) (int64, int64) {
    split_chunk := strings.Split(location, ":")
    start, _ := strconv.ParseInt(split_chunk[0], 10, 64)
    end, _ := strconv.ParseInt(split_chunk[1], 10, 64)
    return start, end
}

// handleFile is called by handler when a file and the boundary are sent via PUT request.
// It is responsible for verifying the md5 of each chunk in the file, replicating it's directory structure as it was on the sender, and writing the file to disk.
func handleFile(bytes_of_file []byte) {
    multipart_reader := multipart.NewReader(bytes.NewReader(bytes_of_file), util.MULTIPART_BOUNDARY)
    for {
        next_part, end_of_file := multipart_reader.NextPart()
        if end_of_file != nil { // Reached end of multipart file
            break
        }
        chunk_path := next_part.Header.Get("name")
        write_path := chunk_path + ".tmp"
        chunk_start, chunk_end := getChunkLocation(next_part.Header.Get("location"))
        chunk_size := chunk_end - chunk_start
        bytes_of_chunk := make([]byte, chunk_size)
        next_part.Read(bytes_of_chunk)
        if next_part.Header.Get("md5") != util.GenerateMD5(bytes_of_chunk) {
            // validation failed, request this specific chunk again using chunk size
            fmt.Println("Bad chunk of " + next_part.Header.Get("name") + " from bytes " + next_part.Header.Get("location"))
            new_bytes := requestPart(next_part.Header.Get("name"), next_part.Header, chunk_start, chunk_end)
            handleFile(new_bytes)
        } else {
            // Validation succeeded, write chunk to disk
            _, err := os.Open(write_path)
            if os.IsNotExist(err) {
                // The file which this chunk belongs to does not already exist. Create a new empty file and companion file.
                total_size, _ := strconv.ParseInt(next_part.Header.Get("total_size"), 10, 64)
                createEmptyFile(chunk_path, total_size)
            }
            // Write bytes of chunk to specific location in file, append chunk md5 to companion file.
            new_fi, _ := os.OpenFile(write_path, os.O_APPEND|os.O_WRONLY, 0600)
            num, _ := new_fi.WriteAt(bytes_of_chunk, chunk_start)
            fmt.Println("Wrote ", num, " bytes of chunk")
            new_fi.Close()
            addPartToCompanion(chunk_path, next_part.Header.Get("md5"), next_part.Header.Get("location"))
            if isFileComplete(chunk_path) {
                fmt.Println("Fully assembled ", chunk_path)
                os.Rename(write_path, chunk_path)
                os.Remove(chunk_path + ".comp")
                removeFromCache(chunk_path)
            }
        }
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
func requestPart(path string, part_header textproto.MIMEHeader, start int64, end int64) []byte {
    post_url := fmt.Sprintf("http://localhost:8080/get_file.go?name=%s&start=%d&end=%d", path, start, end)
    client := http.Client{}
    request, _ := http.NewRequest("POST", post_url, nil)
    resp, err := client.Do(request)
    body_bytes, _ := ioutil.ReadAll(resp.Body)
    if err != nil {
        // MODIFY behavior here later. Currently just tries again every 5 seconds.
        time.Sleep(5 * time.Second)
        return requestPart(path, part_header, start, end)
    }
    byte_buffer := bytes.Buffer{}
    multipart_writer := multipart.NewWriter(&byte_buffer)
    multipart_writer.SetBoundary(util.MULTIPART_BOUNDARY)
    part, _ := multipart_writer.CreatePart(part_header)
    part.Write(body_bytes)
    multipart_writer.Close()
    return byte_buffer.Bytes()
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

// main is the entry point of the webserver. It is responsible for registering handlers and beginning the request serving loop.
func main() {
    companion_lock = false
    http.HandleFunc("/send.go", handler)
    http.HandleFunc("/", errorHandler)
    http.ListenAndServe(":8081", nil)
}
