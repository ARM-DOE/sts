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
type Companion struct {
    Path         string
    TotalSize    int64
    CurrentParts []string
}

// decodeCompanion takes the path of the "final file", decodes, and
// returns the companion struct that can be found at that path.
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
// addPartToCompanion takes an argument "path" that represents where the file that the companion is representing is stored.
func AddPartToCompanion(path string, id string, location string) {
    CompanionLock.Lock()
    companion := DecodeCompanion(path)
    companion_addition := id + ";" + location
    if !IsStringInArray(companion.CurrentParts, companion_addition) {
        companion.CurrentParts = append(companion.CurrentParts, companion_addition)
    }
    companion.EncodeAndWrite()
    CompanionLock.Unlock()
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

// newCompanion creates a new companion file initialized
// with specified parameters, and writes it to disk.
func NewCompanion(path string, size int64) Companion {
    if &CompanionLock == nil {
        CompanionLock = sync.Mutex{}
    }
    current_parts := make([]string, 0)
    new_companion := Companion{path, size, current_parts}
    new_companion.EncodeAndWrite()
    return new_companion
}
