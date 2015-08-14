package main

import (
    "os"
    "time"
    "util"
)

// Cache is a wrapper around Listener that provides extra Bin-filling functionality.
type Cache struct {
    files_available bool
    bin_channel     chan Bin
    bin_size        int64
    watch_dir       string
    listener        *util.Listener
    senders         []*Sender
}

// CacheFactory creates and returns a new Cache struct with default values.
func CacheFactory(cache_file_name string, watch_dir string, bin_channel chan Bin, bin_size int64) *Cache {
    new_cache := &Cache{}
    new_cache.files_available = false
    new_cache.watch_dir = watch_dir
    new_cache.bin_size = bin_size
    new_cache.bin_channel = bin_channel
    new_cache.senders = nil
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

// freeSender checks the Busy value of all senders, and returns true if there are any non-busy Senders.
func (cache *Cache) freeSender() bool {
    for _, sender := range cache.senders {
        if !sender.Busy {
            return true
        }
    }
    return false
}

// allocate is called when a new file is detected.
// While there are still any unallocated bytes in the cache, allocate continuously fills new Bins.
// When a new non-empty Bin is filled, it is sent down the Bin channel.
// Allocate stops when a Bin is filled, but is empty.
func (cache *Cache) allocate() {
    cache.files_available = true
    for cache.files_available {
        if !cache.freeSender() { // There's no open sender to accept this bin
            time.Sleep(100 * time.Millisecond)
            continue
        }
        new_bin := BinFactory(cache.bin_size, cache.watch_dir)
        new_bin.fill(cache)
        if new_bin.Empty {
            cache.files_available = false
        } else {
            cache.bin_channel <- new_bin
        }
    }
}

// scan blocks until it receives a file addition from the cache listener.
// When an addition is detected, it calls cache.allocate()
func (cache *Cache) scan() {
    update_channel := make(chan string, 1)
    go cache.listener.Listen(update_channel)
    for {
        new_path := <-update_channel
        cache.listener.Files[new_path] = 0
        cache.allocate()
    }
}

func (cache *Cache) SetSenders(senders []*Sender) {
    cache.senders = senders
}
