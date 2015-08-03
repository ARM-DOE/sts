package main

import (
    "bytes"
    "crypto/md5"
    "fmt"
    "mime/multipart"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "strings"
)

// errorHandler is called when any page that is not a registered API method is requested.
func errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}

// handler receives a multipart file from the sender via PUT request, with an added HTTP header that specifies the boundary used to split the multipart file.
// handler calls handleFile as a goroutine, which validates and writes the file to disk.
func handler(w http.ResponseWriter, r *http.Request) {
    byte_buffer := bytes.Buffer{}
    byte_buffer.ReadFrom(r.Body)
    go handleFile(byte_buffer.Bytes(), r.Header.Get("boundary"))
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
func handleFile(bytes_of_file []byte, boundary string) {
    multipart_reader := multipart.NewReader(bytes.NewReader(bytes_of_file), boundary)
    for {
        next_part, end_of_file := multipart_reader.NextPart()
        if end_of_file != nil { // Reached end of multipart file
            break
        }
        chunk_path := next_part.Header.Get("name")
        _, exists := os.Open(chunk_path)
        if os.IsNotExist(exists) {
            total_size, _ := strconv.ParseInt(next_part.Header.Get("total_size"), 10, 64)
            createNewFile(chunk_path, total_size)
        }
        new_fi, _ := os.OpenFile(chunk_path, os.O_APPEND|os.O_WRONLY, 0600)
        chunk_start, chunk_end := getChunkLocation(next_part.Header.Get("location"))
        chunk_size := chunk_end - chunk_start
        bytes_of_chunk := make([]byte, chunk_size)
        next_part.Read(bytes_of_chunk)

        if next_part.Header.Get("md5") != generateMD5(bytes_of_chunk) {
            // validation failed, request this specific chunk again using chunk size
            fmt.Println("Bad chunk of " + next_part.Header.Get("name") + " from bytes " + next_part.Header.Get("location"))
        } else {
            num, _ := new_fi.WriteAt(bytes_of_chunk, chunk_start)
            fmt.Println("Wrote ", num, " bytes of chunk")
            new_fi.Close()
        }
    }
}

// createNewFile is called when a multipart chunk is encountered and the whole file hasn't been created on the reciever yet.
// It fills a file with zero value bytes so that the created file is equal to the final size of multipart file.
func createNewFile(path string, size int64) {
    os.MkdirAll(filepath.Dir(path), os.ModePerm)
    fi, _ := os.Create(path)
    fi.Truncate(size)
    fi.Close()
}

// generateMD5 generates and returns an md5 string from an array of bytes.
func generateMD5(data []byte) string {
    new_hash := md5.New()
    new_hash.Write(data)
    return fmt.Sprintf("%x", new_hash.Sum(nil))
}

// main is the entry point of the webserver. It is responsible for registering handlers and beginning the request serving loop.
func main() {
    http.HandleFunc("/send.go", handler)
    http.HandleFunc("/", errorHandler)
    http.ListenAndServe(":8081", nil)
}
