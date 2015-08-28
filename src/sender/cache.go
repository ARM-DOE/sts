package sender

import (
    "os"
    "sync"
    "time"
    "util"
)

// Cache is a wrapper around Listener that provides extra Bin-filling functionality.
type Cache struct {
    bin_channel  chan Bin       // The channel that newly filled Bins should be passed into.
    bin_size     int64          // Default Bin size. Obtained from config.
    watch_dir    string         // Watch directory, used to create listener and new bins.
    listener     *util.Listener // The instance of listener that the cache uses to store data and get new files.
    senders      []*Sender      // A list of all the active senders that the cache can query when creating new Bins.
    channel_lock sync.Mutex     // As soon as a new file is obtained from the listener, this lock is applied. It stops calls to cache.allocate coming too early.
}

// NewCache creates and returns a new Cache struct with default values.
func NewCache(cache_file_name string, watch_dir string, bin_size int64, bin_channel chan Bin) *Cache {
    new_cache := &Cache{}
    new_cache.watch_dir = watch_dir
    new_cache.bin_size = bin_size
    new_cache.bin_channel = bin_channel
    new_cache.senders = nil
    new_cache.listener = util.NewListener(cache_file_name, watch_dir)
    new_cache.channel_lock = sync.Mutex{}
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

// removeFile deletes a file and its progress from both the in-memory and local cache.
// It should only be used when a file has been confirmed to have completely sent.
func (cache *Cache) removeFile(path string) {
    delete(cache.listener.Files, getWholePath(path))
    cache.listener.WriteCache()
}

// freeSender checks the Busy value of all senders and returns true if there are any non-busy Senders.
func (cache *Cache) freeSender() bool {
    for _, sender := range cache.senders {
        if !sender.Busy {
            return true
        }
    }
    return false
}

// allocate is called when a new file is detected and on startup.
// While there are still any unallocated bytes in the cache, allocate continuously fills new Bins.
// When a new non-empty Bin is filled, it is sent down the Bin channel.
// Allocate stops when a Bin is filled, but is empty.
func (cache *Cache) allocate() {
    time.Sleep(5 * time.Millisecond) // Give enough time for the cache adder to pick up the lock.
    cache.channel_lock.Lock()
    defer cache.channel_lock.Unlock()
    files_available := true
    for files_available {
        if !cache.freeSender() { // There's no open sender to accept this bin
            time.Sleep(100 * time.Millisecond)
            continue
        }
        new_bin := NewBin(cache.bin_size, cache.watch_dir)
        new_bin.fill(cache)
        if new_bin.Empty {
            files_available = false
        } else {
            cache.bin_channel <- new_bin
        }
    }
}

// scan blocks until it receives a file addition from the cache listener.
// When an addition is detected, it creates a new entry in the cache for the file and calls cache.allocate()
func (cache *Cache) scan() {
    // Load bins and allocate cache on startup
    cache.loadBins()
    cache.allocate()
    update_channel := make(chan string, 1)
    cache.listener.SetOnFinish(cache.allocate)
    go cache.listener.Listen(update_channel)
    for {
        new_path := <-update_channel
        cache.channel_lock.Lock()
        cache.listener.Files[new_path] = 0
        cache.channel_lock.Unlock()
    }
}

func (cache *Cache) BinSize() int64 {
    return cache.bin_size
}

func (cache *Cache) SetSenders(senders []*Sender) {
    cache.senders = senders
}
