package main

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "strings"
    "util"
)

// Part represents a part of a multipart file.
// It contains the data necessary to create a part of a multipart file which can then be parsed on the receiving end.
type Part struct {
    Path      string
    Progress  int64
    Size      int64
    Start     int64
    End       int64
    TotalSize int64
}

// PartFactory creates and returns a new instance of a Part.
// PartFactory requires the following arguments:
// A start position for the Part, relative to the whole file: [0, file.size - 1]
// An end position for the Part, relative to the whole file: [1, file_size]
// The total size of the file, so that a file full of empty bytes can be generated on the other end, no matter which part comes in first.
func PartFactory(path string, start int64, end int64, total_size int64) Part {
    new_part := Part{}
    info, _ := os.Stat(path)
    new_part.Size = info.Size()
    new_part.Path = path
    new_part.Start = start
    new_part.End = end
    new_part.Progress = 0
    new_part.TotalSize = total_size
    return new_part
}

// Bin is a container for Part(s).
// When new files are detected in the watch directory, new Bins are created and filled until all the new files have been allocated.
// The filled Bins are passed down the bin channel to senders, which construct and send a multipart file from the list of Parts.
type Bin struct {
    Files     []Part
    WatchDir  string
    Size      int64
    BytesLeft int64
    Empty     bool
    Name      string
}

// BinFactory creates a new empty Bin object.
// BinFactory takes a size argument, which specifies how many bytes the Bin can hold before it is designated as full.
// It also takes argument watch_dir, which allows the Bin to generate a path where the files will be stored on the receiving end.
func BinFactory(size int64, watch_dir string) Bin {
    new_bin := Bin{}
    new_bin.Size = size
    new_bin.BytesLeft = size
    new_bin.Files = make([]Part, 0)
    new_bin.WatchDir = watch_dir
    new_bin.Empty = true
    return new_bin
}

// loadBins allows unfinished Bins to continue sending after an unexpected shutdown.
// After a Bin is finished sending, it is written to a file, so that file allocations are not lost.
// On startup, all Bin files are read, and any unfinished Bins are loaded into memory, after which the Senders may continue to process them.
func (cache *Cache) loadBins() {
    filepath.Walk("bins", cache.walkBin)
}

// walkBin is called by loadBins for every file in the "bins" directory.
// Whenever a file path ending in .bin is encountered, walkBin will pass it to loadBin for deserialization.
// Each deserialized Bin is then passed to the Bin channel.
func (cache *Cache) walkBin(path string, info os.FileInfo, err error) error {
    if strings.HasSuffix(path, ".bin") {
        bin_file, _ := ioutil.ReadFile(path)
        loaded_bin := cache.loadBin(bin_file)
        cache.bin_channel <- loaded_bin
    }
    return nil
}

// loadBin is called when a serialized bin file is found that needs to be loaded into memory.
// loadBin takes an array of bytes, uses the gob deserializer, and returns the decoded Bin.
func (cache *Cache) loadBin(bin_bytes []byte) Bin {
    bin_buffer := bytes.NewBuffer(bin_bytes)
    decoded_bin := Bin{}
    bin_decoder := gob.NewDecoder(bin_buffer)
    eff := bin_decoder.Decode(&decoded_bin)
    if eff != nil {
        fmt.Println(eff.Error())
    }
    return decoded_bin
}

// save dumps an in-memory Bin to a local file in the directory "bins" with filename md5_of(bin.Files)+.bin
func (bin *Bin) save() {
    byte_buffer := new(bytes.Buffer)
    bin_encoder := gob.NewEncoder(byte_buffer)
    bin_md5 := util.GenerateMD5([]byte(fmt.Sprintf("%v", bin.Files)))
    bin.Name = "bins/" + bin_md5 + ".bin"
    err := bin_encoder.Encode(bin)
    if err != nil {
        println(err.Error())
    }
    ioutil.WriteFile(bin.Name+".tmp", byte_buffer.Bytes(), 0700)
    os.Rename(bin.Name+".tmp", bin.Name)
}

func (bin *Bin) delete() {
    os.Remove(bin.Name)
}

// fill iterates through files in the cache until it finds one that is not completely allocated.
// After finding a file, it tries to add as much of the file as possible to the Bin.
// If the Bin has enough space for the whole file, it will continue looking for and adding unallocated files until it is full.
// After a bin is filled, the local cache will be updated.
func (bin *Bin) fill(cache *Cache) {
    for path, allocation := range cache.files {
        if bin.BytesLeft == 0 {
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
            bin.BytesLeft = bin.BytesLeft - added_bytes
            bin.addPart(path, allocation, allocation+added_bytes, info)
            cache.updateFile(path, allocation+added_bytes, info)
            bin.Empty = false
        }
    }
    if !bin.Empty {
        bin.save()
    }
    cache.writeCache()
}

// fitBytes checks a file to see how much of that file can fit inside a Bin.
// fitBytes takes argument allocation, which specifies how many bytes of the file have already been allocated to a Bin.
// fitBytes returns either all the bytes in the file, or how many can fit into the Bin.
func (bin *Bin) fitBytes(allocation int64, file_size int64) int64 {
    unallocated_bytes := file_size - allocation
    if unallocated_bytes > bin.BytesLeft {
        // Can't fit the whole file, return everything left in the bin.
        return bin.BytesLeft
    } else {
        // The file does fit, return it's size
        return unallocated_bytes
    }
}

// addPart calls PartFactory and appends the new part to the Bin.
// See documentation for PartFactory for an indepth explanation of addParts' arguments.
func (bin *Bin) addPart(path string, start int64, end int64, info os.FileInfo) {
    new_part := PartFactory(path, start, end, info.Size())
    bin.Files = append(bin.Files, new_part)
}
