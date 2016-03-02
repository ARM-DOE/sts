package sender

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"util"
)

const TRANSFER_HTTP = "http"
const TRANSFER_DISK = "disk"
const TRANSFER_GRIDFTP = "gridftp"

var config util.Config
var error_log util.Logger
var send_log util.Logger

// Main is the entry point for the sender program.
// It parses config values that are necessary during runtime,
// dispatches the listening and sending threads, and loops infinitely.
func Main(config_file string) {
	// Parse config
	var parse_err error
	config, parse_err = util.ParseConfig(config_file)
	if parse_err != nil {
		fmt.Println("Couldn't parse config", parse_err.Error())
	}
	// Create loggers
	send_log = util.NewLogger(config.Logs_Directory, 1)
	error_log = util.NewLogger(config.Logs_Directory, 4)
	// Create disk manager and attempt to find disk
	disk_manager := util.CreateDiskManager(config.Input_Directory, config.Disk_Switching)
	// Create the channel through which new bins will be sent from the sender to the receiver.
	bin_channel := make(chan Bin, config.Sender_Threads+5) // Create a bin channel with buffer size large enough to accommodate all sender threads and a little wiggle room.
	// Create and start cache file handler and poller
	file_cache := NewCache(config.Cache_File_Name, config.Input_Directory, config.Cache_Write_Interval, config.BinSize(), bin_channel)
	cache_err := file_cache.listener.LoadCache()
	if cache_err != nil {
		error_log.LogError("Could not load cache:", cache_err.Error())
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
	fmt.Println("Ready to send")
	for {
		checkReload(file_cache)
		time.Sleep(1 * time.Second)
	}
}

// getStorePath returns the path that the receiver should use to store a file.
// Given parameters full_path and watch_directory, it will remove watch directory from the full path.
func getStorePath(full_path string, watch_directory string) string {
	store_path := strings.Replace(full_path, filepath.Dir(watch_directory)+util.Sep, "", 1)
	return store_path
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
			error_log.LogError("Couldn't parse config file, changes not accepted:", parse_err.Error())
			config.Reloaded()
			return
		}
		config = temp_config
		if config.StaticDiff(old_config) {
			error_log.LogError("Static config value(s) changed, restarting...")
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

// getWholePath returns the absolute path of the file given the path where the file will be stored on the receiver.
func getWholePath(store_path string) string {
	var abs_path string
	if filepath.IsAbs(config.Input_Directory) {
		abs_path = fmt.Sprintf("%s%c%s", filepath.Dir(config.Input_Directory), os.PathSeparator, store_path)
	} else {
		var abs_err error
		abs_path, abs_err = filepath.Abs(store_path)
		if abs_err != nil {
			error_log.LogError(abs_err.Error())
		}
	}
	return abs_path
}

// checkWatchDir is called on the directory to be watched.
// It validates that the directory exists, and returns the absolute path.
func checkWatchDir(watch_dir string) string {
	_, err := os.Stat(watch_dir)
	var abs_dir string
	if os.IsNotExist(err) {
		error_log.LogError("Watch directory does not exist")
		os.Exit(1)
	}
	if !filepath.IsAbs(watch_dir) {
		var abs_err error
		abs_dir, abs_err = filepath.Abs(watch_dir)
		if abs_err != nil {
			error_log.LogError(fmt.Sprintf("Could not generate abs path for watch directory %s: %s", watch_dir, abs_err.Error()))
		}
	} else {
		abs_dir = watch_dir
	}
	return abs_dir
}
