package sender

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    "util"
)

// Part represents a part of a multipart file.
// It contains the data necessary to create a part of a
// multipart file which can then be parsed on the receiving end.
type Part struct {
    Path      string // Absolute path to the file that Part represents
    Progress  int64  // For keeping track of how much of the Bin has been sent
    Start     int64  // Beginning byte in the part
    End       int64  // Ending byte in the part
    TotalSize int64  // Total size of the file that the Part represents
    MD5       string // MD5 of the data in the part file from Start:End
}

// NewPart creates and returns a new instance of a Part.
// NewPart requires the following arguments:
// A start position for the Part, relative to the whole file: [0, file.size - 1]
// An end position for the Part, relative to the whole file: [1, file_size]
// The total size of the file, so that a file full of empty bytes can be
// generated on the other end, no matter which part comes in first.
func NewPart(path string, start int64, end int64, total_size int64) Part {
    new_part := Part{}
    new_part.TotalSize = total_size
    new_part.Path = path
    new_part.Start = start
    new_part.End = end
    new_part.Progress = 0
    return new_part
}

// getMD5 uses StreamMD5 to digest the file part that the
// Part instance refers to and update its Part.MD5 value.
func (part *Part) getMD5() {
    fi, _ := os.Open(part.Path)
    fi.Seek(part.Start, 0)
    md5_stream := util.NewStreamMD5()
    file_buff := make([]byte, md5_stream.BlockSize)
    reads_necessary := part.countReads(md5_stream.BlockSize)
    for i := int64(0); i < reads_necessary; i++ {
        if i == reads_necessary-1 {
            file_buff = make([]byte, (part.End-part.Start)%int64(md5_stream.BlockSize))
        }
        fi.Read(file_buff)
        md5_stream.Update(file_buff)
    }
    part.MD5 = md5_stream.SumString()
}

// countReads returns an int that represents the number of calls to Read()
// it will take to read in an entire Part, given the size of the byte
// buffer being passed to Read()
func (part *Part) countReads(block_size int) int64 {
    part_size := part.End - part.Start
    if part_size%int64(block_size) == 0 {
        return part_size / int64(block_size)
    }
    return (part_size / int64(block_size)) + 1
}

// Bin is a container for Part(s).
// When new files are detected in the watch directory, new Bins are created and
// filled until all the new files have been allocated. The filled Bins are passed down the bin channel
// to senders, which construct and send a multipart file from the list of Parts.
type Bin struct {
    Files          []Part // All the instances of Part that the Bin contains
    Size           int64  // Maximum amount of bytes that the Bin is able to store
    BytesLeft      int64  // Unallocated bytes in the Bin
    Name           string // MD5 of Bin.Files creates unique Bin name for writing to disk
    TransferMethod string
    WatchDir       string
    Empty          bool
}

// NewBin creates a new empty Bin object.
// NewBin takes a size argument, which specifies
// how many bytes the Bin can hold before it is designated as full.
// It also takes argument watch_dir, which allows the Bin to generate a path
// where the files will be stored on the receiving end.
func NewBin(size int64, watch_dir string) Bin {
    new_bin := Bin{}
    new_bin.TransferMethod = TRANSFER_HTTP
    new_bin.Size = size
    new_bin.BytesLeft = size
    new_bin.Files = make([]Part, 0)
    new_bin.WatchDir = watch_dir
    new_bin.Empty = true
    return new_bin
}

// loadBins allows unfinished Bins to continue sending after an unexpected shutdown.
// After a Bin is finished sending, it is written to a file, so that file allocations are not lost.
// On startup, all Bin files are read, and any unfinished Bins are loaded into memory, after which
// the Senders may continue to process them.
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
// loadBin takes an array of bytes, uses the json decoder, and returns the decoded Bin.
func (cache *Cache) loadBin(bin_bytes []byte) Bin {
    decoded_bin := Bin{}
    decode_err := json.Unmarshal(bin_bytes, &decoded_bin)
    if decode_err != nil {
        fmt.Println(decode_err.Error())
    }
    return decoded_bin
}

// save dumps an in-memory Bin to a local file in the directory "bins" with filename Bin.Name+.bin
func (bin *Bin) save() {
    bin_md5 := util.GenerateMD5([]byte(fmt.Sprintf("%v", bin.Files)))
    bin.Name = "bins/" + bin_md5 + ".bin"
    json_bytes, encode_err := json.Marshal(bin)
    if encode_err != nil {
        println(encode_err.Error())
    }
    ioutil.WriteFile(bin.Name+".tmp", json_bytes, 0700)
    os.Rename(bin.Name+".tmp", bin.Name)
}

// delete removes the local copy of the bin from disk.
func (bin *Bin) delete() {
    err := os.Remove(bin.Name)
    if err != nil {
        fmt.Println("Failed to remove bin " + bin.Name)
    }
}

// fill iterates through files in the cache until it finds one that is not completely allocated.
// After finding a file, it uses shouldAllocate() to see if the data should be added to the Bin.
// It repeats this process until either the Bin runs out of room, or there are no more valid & unallocated files.
// After a bin is filled, the local cache will be updated.
func (bin *Bin) fill(cache *Cache) {
    keep_trying := true // There may still some files in the cache that have been passed over due to selecting of higher priority data
    for keep_trying {   // This outer for loop keeps looping until no unallocated data can be found.
        keep_trying = false // Set keep_trying to false until a file is found that will allow the loop to continue.
        for path, allocation := range cache.listener.Files {
            if bin.BytesLeft == 0 {
                break // Bin is full
            }
            if path == "__TIMESTAMP__" {
                continue // Don't try to allocate the timestamp data
            }
            tag_data := getTag(path)
            info, info_err := os.Stat(path)
            if info_err != nil {
                panic(fmt.Sprintf("File: %s registered in cache, but does not exist", path))
            }
            file_size := info.Size()
            if file_size == 0 {
                // Empty file
                cache.removeFile(path)
            }
            if allocation < file_size && allocation != -1 && shouldAllocate(cache, path) {
                // File should be allocated, add to Bin
                if tag_data.TransferMethod() != TRANSFER_HTTP && bin.Empty {
                    // If the Bin is empty, add any non-standard transfer method files.
                    bin.handleExternalTransferMethod(cache, path, tag_data)
                    break
                }
                added_bytes := bin.fitBytes(allocation, file_size)
                bin.BytesLeft = bin.BytesLeft - added_bytes
                bin.addPart(path, allocation, allocation+added_bytes, info)
                cache.updateFile(path, allocation+added_bytes, info)
            }
            if allocation != -1 {
                keep_trying = true
            }
        }
    }
    if !bin.Empty {
        bin.save()
    }
    cache.listener.WriteCache()
}

// shouldAllocate checks if the given file should be allowed to be saved to a Bin.
// The file will not be allocated if there are higher priority files which haven't
// been allocated yet, or if it isn't the oldest file in the cache with the same tag.
func shouldAllocate(cache *Cache, path string) bool {
    tag_data := getTag(path)
    highest_priority := highestPriority(cache, tag_data)
    oldest_file := oldestFileInTag(cache, path)
    should_send := highest_priority && oldest_file
    return should_send
}

// oldestFileInTag checks the modtime of every file in the cache to see
// if there are any older files of the same tag which have not yet been sent.
// If there are no older files in the tag, it returns true.
func oldestFileInTag(cache *Cache, path string) bool {
    stat, _ := os.Stat(path)
    modtime := stat.ModTime()
    for cache_path, allocation := range cache.listener.Files {
        if allocation != -1 && sameTag(path, cache_path) {
            cache_stat, _ := os.Stat(cache_path)
            cache_modtime := cache_stat.ModTime()
            if modtime.Before(cache_modtime) {
                return false
            }
        }
    }
    return true
}

// sameTag returns true if two file paths have the same tag.
func sameTag(path1 string, path2 string) bool {
    tag1 := strings.Split(path1, ".")[0]
    tag2 := strings.Split(path2, ".")[0]
    if tag1 == tag2 {
        return true
    }
    return false
}

// anyHigherPriority checks if there is any unallocated file in the cache that has a
// higher send priority than the given TagData. For the higher priority file to be registered,
// it must have the same transfer method as the passed in TagData
func highestPriority(cache *Cache, tag_data util.TagData) bool {
    for path, allocation := range cache.listener.Files {
        cache_tag := getTag(path)
        if allocation != -1 && cache_tag.Priority < tag_data.Priority && cache_tag.TransferMethod() == tag_data.TransferMethod() {
            return false
        }
    }
    return true
}

// getTag returns a TagData instance for the first tag pattern that matches the file path.
// If no tag pattern matches, it returns the default TagData.
func getTag(path string) util.TagData {
    path_tag := strings.Split(path, ".")[0]
    for tag_pattern, tag_data := range config.Tags {
        if tag_pattern == "DEFAULT" {
            continue // Don't check default tag
        }
        matched, _ := regexp.MatchString(tag_pattern, path_tag)
        if matched {
            return tag_data
        }
    }
    return config.Tags["DEFAULT"]
}

// handleExternalTranferMethod is called when a non-HTTP, unallocated file is found, and the
// currently allocating Bin is empty. It calls either handleDisk or handleGridFTP depending
// on the transfer method of the file.
func (bin *Bin) handleExternalTransferMethod(cache *Cache, path string, tag_data util.TagData) {
    info, _ := os.Stat(path)
    switch tag_data.TransferMethod() {
    case TRANSFER_HTTP:
        panic("handleExternalTransferMethod called, but method is not external")
    case TRANSFER_DISK:
        bin.handleDisk(path, tag_data)
    case TRANSFER_GRIDFTP:
        bin.handleGridFTP(path, tag_data)
    default:
        panic(fmt.Sprintf("Transfer method %s not recognized", tag_data.TransferMethod()))
    }
    cache.updateFile(path, -1, info)
}

// handleDisk is called to prep bins for files with transfer method disk.
func (bin *Bin) handleDisk(path string, tag_data util.TagData) {
    bin.TransferMethod = TRANSFER_DISK
    info, _ := os.Stat(path)
    bin.Size = info.Size()
    bin.BytesLeft = 0
    bin.addPart(path, 0, info.Size(), info)
}

// handleGridFTP is called to prep bins for files with transfer method GridFTP.
func (bin *Bin) handleGridFTP(path string, tag_data util.TagData) {
    bin.TransferMethod = TRANSFER_GRIDFTP
    info, _ := os.Stat(path)
    bin.Size = info.Size()
    bin.BytesLeft = 0
    bin.addPart(path, 0, info.Size(), info)
}

// fitBytes checks a file to see how much of that file can fit inside a Bin.
// It takes argument allocation, which specifies how many bytes of the file have already been allocated to a Bin.
// fitBytes returns either all the bytes in the file, or how many can fit into the Bin.
func (bin *Bin) fitBytes(allocation int64, file_size int64) int64 {
    unallocated_bytes := file_size - allocation
    if unallocated_bytes > bin.BytesLeft {
        // Can't fit the whole file, return everything left in the bin.
        return bin.BytesLeft
    } else {
        // The file does fit, return its size
        return unallocated_bytes
    }
}

// addPart calls NewPart and appends the new part to the Bin.
// See documentation for NewPart for an in-depth explanation of addParts arguments.
func (bin *Bin) addPart(path string, start int64, end int64, info os.FileInfo) {
    new_part := NewPart(path, start, end, info.Size())
    bin.Files = append(bin.Files, new_part)
    bin.Empty = false
}
