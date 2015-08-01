package main

import (
    "os"
    "path/filepath"
    "strings"
)

// Part represents a part of a multipart file.
// It contains the data necessary to create a part of a multipart file which can then be parsed on the receiving end.
type Part struct {
    path       string
    progress   int64
    size       int64
    start      int64
    end        int64
    total_size int64
}

// PartFactory creates and returns a new instance of a Part.
// PartFactory requires the following arguments:
// A start position for the Part, relative to the whole file: [0, file.size - 1]
// An end position for the Part, relative to the whole file: [1, file_size]
// The total size of the file, so that a file full of empty bytes can be generated on the other end, no matter which part comes in first.
func PartFactory(path string, start int64, end int64, total_size int64) Part {
    new_part := Part{}
    info, _ := os.Stat(path)
    new_part.size = info.Size()
    new_part.path = path
    new_part.start = start
    new_part.end = end
    new_part.progress = 0
    new_part.total_size = total_size
    return new_part
}

// Bin is a container for Part(s).
// When new files are detected in the watch directory, new Bins are created and filled until all the new files have been allocated.
// The filled Bins are passed down the bin channel to senders, which construct and send a multipart file from the list of Parts.
type Bin struct {
    files      []Part
    watch_dir  string
    size       int64
    bytes_left int64
    empty      bool
}

// BinFactory creates a new empty Bin object.
// BinFactory takes a size argument, which specifies how many bytes the Bin can hold before it is designated as full.
// It also takes argument watch_dir, which allows the Bin to generate a path where the files will be stored on the receiving end.
func BinFactory(size int64, watch_dir string) Bin {
    new_bin := Bin{}
    new_bin.size = size
    new_bin.bytes_left = size
    new_bin.files = make([]Part, 0)
    new_bin.watch_dir = watch_dir
    new_bin.empty = true
    return new_bin
}

// loadBins is an unimplemented function that will allow unfinished Bins to continue sending after an unexpected shutdown.
// After a Bin is finished sending, it will be written to a file, so that file allocations are not lost.
// On startup, the program will read all Bin files, and create unfinished Bins, which may then continue to be processed by Senders.
func (cache *Cache) loadBins() {
    filepath.Walk("bins", cache.loadBin)
}

// loadBin is an unimplemented function that will be called by loadBins.
// loadBin will decode a Bin object from a file, and pass it to the bin channel to be processed.
func (cache *Cache) loadBin(path string, info os.FileInfo, err error) error {
    if strings.HasSuffix(path, ".bin") {
        // Decode bin
    }
    return nil
}

// fill iterates through files in the cache until it finds one that is not completely allocated.
// After finding a file, it tries to add as much of the file as possible to the Bin.
// If the Bin has enough space for the whole file, it will continue looking for and adding unallocated files until it is full.
// After a bin is filled, the local cache will be updated.
func (bin *Bin) fill(cache *Cache) {
    for path, allocation := range cache.files {
        if bin.bytes_left == 0 {
            // Bin is full
            break
        }
        if path == "__TIMESTAMP__" {
            continue
        }
        info, _ := os.Stat(path)
        file_size := info.Size()
        if allocation < file_size && allocation != -1 {
            // File has not already been allocated to another Bin
            added_bytes := bin.fitBytes(allocation, file_size)
            bin.bytes_left = bin.bytes_left - added_bytes
            bin.addPart(path, allocation, allocation+added_bytes, info)
            cache.updateFile(path, allocation+added_bytes, info)
            bin.empty = false
        }
    }
    cache.writeCache()
}

// fitBytes checks a file to see how much of that file can fit inside a Bin.
// fitBytes takes argument allocation, which specifies how many bytes of the file have already been allocated to a Bin.
// fitBytes returns either all the bytes in the file, or how many can fit into the Bin.
func (bin *Bin) fitBytes(allocation int64, file_size int64) int64 {
    unallocated_bytes := file_size - allocation
    if unallocated_bytes > bin.bytes_left {
        // Can't fit the whole file, return everything left in the bin.
        return bin.bytes_left
    } else {
        // The file does fit, return it's size
        return unallocated_bytes
    }
}

// addPart calls PartFactory and appends the new part to the Bin.
// See documentation for PartFactory for an indepth explanation of addParts' arguments.
func (bin *Bin) addPart(path string, start int64, end int64, info os.FileInfo) {
    new_part := PartFactory(path, start, end, info.Size())
    bin.files = append(bin.files, new_part)
}
