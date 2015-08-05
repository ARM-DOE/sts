package main

import (
    "os"
    "util"
)

type Cache struct {
    files_available bool
    bin_channel     chan Bin
    watch_dir       string
    listener        *util.Listener
}

func CacheFactory(cache_file_name string, watch_dir string, bin_channel chan Bin) *Cache {
    new_cache := &Cache{}
    new_cache.files_available = false
    new_cache.watch_dir = watch_dir
    new_cache.bin_channel = bin_channel
    new_cache.listener = util.ListenerFactory(cache_file_name, watch_dir)
    return new_cache
}

// updateFile updates the record of bytes that have been allocated to a Bin.
// Take note that updateFile does NOT update the local cache.
func (cache *Cache) updateFile(path string, new_byte_progress int64, info os.FileInfo) {
    if new_byte_progress == info.Size() {
        new_byte_progress = -1
    }
    if new_byte_progress > info.Size() {
        panic("Bin stored too many bytes")
    }
    cache.listener.Files[path] = new_byte_progress
}

// removeFile deletes a file and it's progress from the cache.
// It should only be used when a file has been confirmed to have completely sent.
func (cache *Cache) removeFile(path string) {
    delete(cache.listener.Files, path)
    cache.listener.WriteCache()
}

// allocate is called when a new file is detected.
// While there are still any unallocated bytes in the cache, allocate continuously fills new Bins.
// When a new non-empty Bin is filled, it is sent down the Bin channel.
// Allocate stops when a Bin is filled, but is empty.
func (cache *Cache) allocate() {
    for cache.files_available {
        new_bin := BinFactory(util.BIN_SIZE, cache.watch_dir)
        new_bin.fill(cache)
        if new_bin.Empty {
            cache.files_available = false
        } else {
            cache.bin_channel <- new_bin
        }
    }
}

func (cache *Cache) scan() {
    update_channel := make(chan string, 1)
    go cache.listener.Listen(update_channel)
    for {
        select {
        case new_path := <-update_channel:
            cache.listener.Files[new_path] = 0
            cache.files_available = true
            cache.allocate()
        }
    }
}
