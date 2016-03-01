package sender

import (
	"strings"
	"time"
	"util"
)

const TRANSFER_HTTP = "http"
const TRANSFER_DISK = "disk"
const TRANSFER_GRIDFTP = "gridftp"

var config *util.Config
var send_log *util.Logger

// Main is the entry point for the sender program.
// It parses config values that are necessary during runtime,
// dispatches the listening and sending threads, and loops infinitely.
func Main(in_config *util.Config) {
	config = in_config

	// Create "send" logger
	send_log = util.NewLogger(config.Logs_Directory, util.LOGGING_SEND, false)

	// Create disk manager and attempt to find disk
	disk_manager := util.CreateDiskManager(config.Input_Directory, config.Disk_Switching)

	// Create the channel through which new bins will be sent from the sender to the receiver.
	bin_channel := make(chan Bin, config.Sender_Threads+5) // Create a bin channel with buffer size large enough to accommodate all sender threads and a little wiggle room.

	// Create and start cache file handler and poller
	file_cache := NewCache(config.Cache_File_Name, config.Input_Directory, config.Cache_Write_Interval, config.BinSize(), bin_channel)
	cache_err := file_cache.listener.LoadCache()
	if cache_err != nil {
		util.LogError("Could not load cache:", cache_err.Error())
	}
	poller := NewPoller(file_cache)
	file_cache.setPoller(poller)
	go poller.poll()

	// Dispatch senders
	senders := make([]*Sender, config.Sender_Threads)
	for dispatched_senders := 0; dispatched_senders < config.Sender_Threads; dispatched_senders++ {
		created_sender := NewSender(disk_manager, file_cache.bin_channel, config.Compression())
		go created_sender.run()
		senders[dispatched_senders] = created_sender
	}
	file_cache.SetSenders(senders)
	go file_cache.scan() // Start the file listener thread

	// Start loop
	util.LogDebug("SENDER Ready")
	for {
		checkReload(file_cache)
		time.Sleep(1 * time.Second)
	}
}

// checkReload is called by the main thread every second to check if any changes have been made to
// the config file to the webserver. If changes are detected, checkReload determines whether to reload
// only dynamic values of whether to perform a full restart. If a full restart is needed, Restart() will
// be called once all sender threads have finished their current Bin.
func checkReload(cache *Cache) {
	if config.ShouldReload() {
		// Update in-memory config
		old_config := config
		temp_config, parse_err := util.ParseConfig(config.FileName())
		if parse_err != nil {
			util.LogError("Failed to parse config file, changes not accepted:", parse_err.Error())
			config.Reloaded()
			return
		}
		config = temp_config
		if config.StaticDiff(old_config) {
			util.LogError("Static config value(s) changed, restarting...")
			cache.setSendersActive(false)
			for cache.anyActiveSender() {
				time.Sleep(1 * time.Second)
			}
			util.Restart()
		} else {
			// Reload dynamic values
			cache.SetBinSize(config.BinSize())
		}
		config.Reloaded()
	}
}

// getStorePath returns the path that the receiver should use to store a file.
// Given parameters full_path and watch_directory, it will remove watch directory from the full path.
func getStorePath(full_path string, watch_directory string) string {
	store_path := strings.Replace(full_path, watch_directory+util.Sep, "", 1)
	return store_path
}

// getWholePath returns the absolute path of the file given the path where the file will be stored on the receiver.
func getWholePath(store_path string) string {
	return util.JoinPath(config.Input_Directory, store_path)
}
