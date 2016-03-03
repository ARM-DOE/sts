package sender

import (
	"os"
	"strings"
	"sync"
	"time"
	"util"
)

// Cache is a wrapper around Listener that provides extra Bin-filling functionality.
type Cache struct {
	allocation_lock sync.Mutex        // Lock in place during bin allocation. Keeps files from being deleted during allocation.
	bin_channel     chan Bin          // The channel that newly filled Bins should be passed into.
	poller          *Poller           // TODO write something about poller
	bin_size        int               // Default Bin size. Obtained from config.
	watch_dir       string            // Watch directory, used to create listener and new bins.
	listener        *util.Listener    // The instance of listener that the cache uses to store data and get new files.
	senders         []*Sender         // A list of all the active senders that the cache can query when creating new Bins.
	md5s            map[string]string // A list of all the pre-calculated md5s
	channel_lock    sync.Mutex        // As soon as a new file is obtained from the listener, this lock is applied. It stops calls to cache.allocate coming too early.
}

// NewCache creates and returns a new Cache struct with default values.
func NewCache(cache_file_name string, watch_dir string, cache_write_interval int64, bin_size int, bin_channel chan Bin) *Cache {
	new_cache := &Cache{}
	new_cache.md5s = make(map[string]string)
	new_cache.watch_dir = watch_dir
	new_cache.allocation_lock = sync.Mutex{}
	new_cache.bin_size = bin_size
	new_cache.bin_channel = bin_channel
	new_cache.senders = nil
	new_cache.listener = util.NewListener(cache_file_name, cache_write_interval, config.Input_Age, watch_dir)
	new_cache.channel_lock = sync.Mutex{}
	return new_cache
}

// updateFileAlloc updates the record of bytes that have been allocated to a Bin.
// Take note that updateFile does NOT update the local cache.
// updateFile takes an optional argument new_timestamp, which should be either true or false,
// depending whether or not a new timestamp should be stored with the file.
// Generally, this would only happen when the file is being updated for the first time.
func (cache *Cache) updateFileAlloc(path string, new_byte_alloc int64, tag_data *util.TagData, info os.FileInfo, new_timestamp ...bool) bool {
	finished := false
	if new_byte_alloc == info.Size() || new_byte_alloc == -1 {
		new_byte_alloc = -1
		finished = true
	}
	if new_byte_alloc > info.Size() {
		util.LogError("Bin tried to store too many bytes:", path)
		// TODO: Should we do something about this? What if a file changes in the middle of being sent?
	}
	// Lock cache so it won't be written or loaded during file update.
	cache.listener.WriteLock.Lock()
	defer cache.listener.WriteLock.Unlock()
	// Update cache file with new byte progress
	cache_file := cache.listener.Files[path]
	if cache_file.Bytes != info.Size() {
		util.LogError("File changed:", path) // TODO: Delete the file from the cache?
	}
	cache_file.Allocation = new_byte_alloc
	if len(new_timestamp) > 0 && new_timestamp[0] == true {
		cache_file.StartTime = util.GetTimestampNS()
	}
	cache.listener.Files[path] = cache_file
	return finished
}

func (cache *Cache) updateFileProgress(path string, bytes_sent int64) bool {
	cache.listener.WriteLock.Lock()
	defer cache.listener.WriteLock.Unlock()
	cache_file := cache.listener.Files[path]
	cache_file.SentBytes += bytes_sent
	if cache_file.SentBytes == cache_file.Bytes {
		// Add to poller for validation/cleanup.
		cache.poller.addFile(path, cache_file.StartTime)
		// Log the sent file.
		send_duration := (time.Now().UnixNano() - cache_file.StartTime) / int64(time.Millisecond)
		receiver_host := config.Receiver_Name
		if receiver_host == "" {
			receiver_host = strings.Split(config.Receiver_Address, ":")[0]
		}
		send_log.LogSend(getStorePath(path, config.Input_Directory), cache.getFileMD5(path), cache_file.Bytes, receiver_host, send_duration)
	}
	return false
}

func (cache *Cache) resendFile(path string) error {
	info, stat_err := os.Stat(path)
	if stat_err != nil {
		return stat_err
	}
	cache.updateFileAlloc(path, 0, nil, info)
	cache.listener.WriteCache()
	return nil
}

// removeFile deletes a file and its progress from both the in-memory and local cache.
// It should only be used when a file has been confirmed to have completely sent.
// It returns the time that the first chunk of the file was allocated to a bin.
// You allocation_lock should be locked when this function is called.
func (cache *Cache) removeFile(path string) int64 {
	cache.listener.WriteLock.Lock()
	item_start := cache.listener.Files[path].StartTime
	delete(cache.listener.Files, path)
	delete(cache.md5s, path)
	cache.listener.WriteLock.Unlock()
	cache.listener.WriteCache()
	return item_start
}

// getFileMD5 generates the MD5 given a file path, and stores it to cache.md5s, so that future parts
// that come from the same file don't have to regenerate the whole file MD5.
// The MD5 is deleted from cache.md5s when the file removal request is processed by the sender webserver.
func (cache *Cache) getFileMD5(path string) string {
	md5, already_calculated := cache.md5s[path]
	if !already_calculated {
		new_md5, md5_err := util.FileMD5(path)
		if md5_err != nil {
			util.LogError(md5_err.Error())
		}
		cache.md5s[path] = new_md5
		return new_md5
	}
	return md5
}

func (cache Cache) copyFileData() map[string]util.CacheFile {
	return cache.listener.Files
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

// setSendersActive sets the active status of all senders to the specified value.
func (cache *Cache) setSendersActive(active bool) {
	for _, sender := range cache.senders {
		sender.Active = active
	}
}

// anyActiveSender checks if any senders are still active or busy, and returns false if they are all idle.
func (cache *Cache) anyActiveSender() bool {
	senders_resting := true
	for _, sender := range cache.senders {
		if sender.Active == true || sender.Busy == true {
			senders_resting = false
		}
	}
	return !senders_resting
}

func (cache *Cache) setPoller(new_poller *Poller) {
	cache.poller = new_poller
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
		new_bin := NewBin(cache, cache.bin_size, cache.watch_dir)
		new_bin.fill()
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
		cache.addPath(new_path)
	}
}

func (cache *Cache) addPath(path string) {
	cache.channel_lock.Lock()
	cache.listener.WriteLock.Lock()
	defer cache.channel_lock.Unlock()
	defer cache.listener.WriteLock.Unlock()
	util.LogDebug("CACHE Found File:", path)
	stat, err := os.Stat(path)
	if err != nil {
		util.LogError("Failed to stat:", path, err.Error())
		return
	}
	fc := util.CacheFile{}
	fc.Bytes = stat.Size()
	cache.listener.Files[path] = fc
}

func (cache *Cache) BinSize() int {
	return cache.bin_size
}

func (cache *Cache) SetBinSize(new_size int) {
	cache.bin_size = new_size
}

func (cache *Cache) SetSenders(senders []*Sender) {
	cache.senders = senders
}
