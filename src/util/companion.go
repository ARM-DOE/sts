package util

import (
    "encoding/json"
    "io/ioutil"
    "os"
    "sync"
)

// companion_lock prevents the same companion file from being written to by multiple threads.
// This is done in order to avoid overwriting newly added part data in companion files.
var CompanionLock sync.Mutex

// Companion is a struct that represents the data of a JSON companion file.
// The methods related to companion files usually operate independently from
// an object instance since companions are generally stored on disk.
type Companion struct {
    Path         string   // The path to the file that the companion is describing.
    TotalSize    int64    // The sum of part bytes received so far.
    SenderName   string   // The hostname and port of the sender that sent the first part of the file.
    CurrentParts []string // A listing of parts received so far. Format for each string: md5;first_byte:last_byte
    File_MD5     string   // The MD5 of the whole file, as reported by the sender.
}

// newCompanion creates a new companion file initialized
// with specified parameters and writes it to disk.
func NewCompanion(path string, size int64, host_name ...string) Companion {
    if &CompanionLock == nil {
        CompanionLock = sync.Mutex{}
    }
    new_companion := Companion{}
    new_companion.Path = path
    new_companion.TotalSize = size
    new_companion.CurrentParts = make([]string, 0)
    if len(host_name) > 0 {
        new_companion.SenderName = host_name[0]
    }
    new_companion.EncodeAndWrite()
    return new_companion
}

// decodeCompanion takes the path of the file that the companion represents, decodes,
// and returns the companion struct that can be found at that path.
func DecodeCompanion(path string) *Companion {
    path = path + ".comp"
    new_companion := &Companion{}
    companion_bytes, _ := ioutil.ReadFile(path)
    json.Unmarshal(companion_bytes, new_companion)
    return new_companion
}

// addPartToCompanion decodes a companion struct, adds the specified id to CurrentParts
// (id must be unique, or it will be ignored) and writes the modified companion struct back to disk.
// It uses a mutex lock to prevent the same companion file being written to by two goroutines at the same time.
// path points to the file the companion represents.
// id is the ID that will be added to the JSON data.
// location is the location header that contains both the start and end bytes of the part.
// file_md5 is the md5 of the whole file according to the sender.
func AddPartToCompanion(path string, id string, location string, file_md5 string) {
    CompanionLock.Lock()
    defer CompanionLock.Unlock()
    companion := DecodeCompanion(path)
    companion.File_MD5 = file_md5
    companion_addition := id + ";" + location
    if !IsStringInArray(companion.CurrentParts, companion_addition) {
        companion.CurrentParts = append(companion.CurrentParts, companion_addition)
    }
    companion.EncodeAndWrite()
}

// encodeAndWrite takes the in-memory representation of a companion file,
// creates a JSON representation, and writes it to disk.
func (comp *Companion) EncodeAndWrite() {
    companion_bytes, _ := json.Marshal(comp)
    comp_file, _ := os.OpenFile(comp.Path+".comp.tmp", os.O_RDWR|os.O_CREATE, 0700)
    comp_file.Write(companion_bytes)
    comp_file.Close()
    os.Rename(comp.Path+".comp.tmp", comp.Path+".comp")
}
