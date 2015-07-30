package main

import (
    "bytes"
    "crypto/md5"
    "fmt"
    "mime/multipart"
    "net/http"
    "os"
    "path/filepath"
    "regexp"
    "strconv"
    "strings"
)

// errorHandler is called when any page that is not a registered API method is requested.
func errorHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, http.StatusNotFound)
}

// handler receives a file from the sender via PUT request, with an added HTTP header with key "metadata" that describes the file.
// Format of "metadata": {"file": {"path": file_path_on_sender, "md5": file_md5, "size": file_size_in_bytes, "modtime": modtime_with_timezone, "store_path": file_path_to_store_at}}
// handler calls handleFile as a goroutine, which validates and writes the file to disk.
func handler(w http.ResponseWriter, r *http.Request) {
    byte_buffer := bytes.Buffer{}
    byte_buffer.ReadFrom(r.Body)
    chunk_size, _ := strconv.ParseInt(r.Header.Get("chunk_size"), 10, 64)
    go handleFile(byte_buffer.Bytes(), r.Header.Get("metadata"), r.Header.Get("boundary"), chunk_size)
}

// getChunkLocation parses the "location" file Header and returns the first and last byte from the chunk.
func getChunkLocation(location string) (int64, int64) {
    split_chunk := strings.Split(location, ":")
    start, _ := strconv.ParseInt(split_chunk[0], 10, 64)
    end, _ := strconv.ParseInt(split_chunk[1], 10, 64)
    return start, end
}

// handleFile is called by handler when a file and the metadata are sent via PUT request.
// It is responsible for verifying the md5 of the file, replicating it's directory structure as it was on the sender, and writing the file to disk.
func handleFile(bytes_of_file []byte, metadata string, boundary string, chunk_size int64) {
    decoded_metadata := decodeJSON(metadata)
    os.MkdirAll(filepath.Dir(decoded_metadata["store_path"]), os.ModePerm)
    new_fi, _ := os.Create(decoded_metadata["store_path"])
    multipart_reader := multipart.NewReader(bytes.NewReader(bytes_of_file), boundary)
    for {
        next_part, end_of_file := multipart_reader.NextPart()
        if end_of_file != nil { // Reached end of multipart file
            break
        }
        chunk_start, _ := getChunkLocation(next_part.Header.Get("location"))
        bytes_of_chunk := make([]byte, chunk_size)
        next_part.Read(bytes_of_chunk)
        if next_part.Header.Get("md5") != generateMD5(bytes_of_chunk) {
            // validation failed, request this specific chunk again using chunk size
            fmt.Println("Bad chunk of " + next_part.Header.Get("name") + "from bytes " + next_part.Header.Get("location"))
        } else {
            new_fi.WriteAt(bytes_of_chunk, chunk_start)
            chunk_start++
        }
    }
    new_fi.Close()
    fmt.Println("Wrote " + decoded_metadata["store_path"] + " to disk")
}

// validateMultipartFile is capable of reading multipart files outputted by chunkFile in the sender program.
// For each chunk in the multipart file, the md5 is verified, if a bad chunk is detected, the chunk could be reobtained from the webserver in the sender.
func validateMultipartFile(bytes_of_file []byte, boundary string, chunk_size int64) bool {
    file_is_good := true
    multipart_reader := multipart.NewReader(bytes.NewReader(bytes_of_file), boundary)
    for {
        next_part, end_of_file := multipart_reader.NextPart()
        if end_of_file != nil { // Reached end of multipart file
            return file_is_good
        }
        bytes_of_chunk := make([]byte, chunk_size)
        next_part.Read(bytes_of_chunk)
        if next_part.Header.Get("md5") != generateMD5(bytes_of_chunk) {
            // validation failed, request this specific chunk again using chunk size
            fmt.Println("Bad chunk of " + next_part.Header.Get("name") + "from bytes " + next_part.Header.Get("location"))
            file_is_good = false
        }
    }
    return file_is_good
}

// generateMD5 generates and returns an md5 string from an array of bytes.
func generateMD5(data []byte) string {
    new_hash := md5.New()
    new_hash.Write(data)
    return fmt.Sprintf("%x", new_hash.Sum(nil))
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
