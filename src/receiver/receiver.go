package receiver

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"util"
)

// finalize_mutex prevents the cache from updating its timestamp while files
// from its addition channel are being processed.
var finalize_mutex sync.Mutex

var config *util.Config

var receiver_log *util.Logger
var disk_log *util.Logger

// log_map stores recently completed files, to be accessed by polling. If a key points to a value
// of -1, that file has been validated and can be removed. If the value is not -1, it will be a
// positive number that represents how many times the sender has asked about that file. Higher numbers
// will increase the search width in the logs.
var log_map map[string]bool
var log_map_lock sync.Mutex

const DEBUG = false
const LOG_SEARCH_WIDTH = 30

// Main is the entry point of the webserver. It is responsible for registering HTTP
// handlers, parsing the config file, starting the file listener, and beginning the request serving loop.
func Main(in_config *util.Config) {
	config = in_config

	// Setup mutex locks and parse config into global variable.
	finalize_mutex = sync.Mutex{}
	util.CompanionLock = sync.Mutex{}

	// Setup loggers
	receiver_log = util.NewLogger(config.Logs_Directory, util.LOGGING_RECEIVE, false)
	disk_log = util.NewLogger(config.Logs_Directory, util.LOGGING_DISK, false)

	// Initialize log_map to store validated files
	log_map = make(map[string]bool)
	log_map_lock = sync.Mutex{}

	// Setup listener and add ignore patterns.
	addition_channel := make(chan string, 1)
	listener := util.NewListener(config.Cache_File_Name, config.Cache_Write_Interval, config.Staging_Directory, config.Output_Directory)
	listener.SetOnFinish(onFinish)
	listener.AddIgnored(`\.tmp`)
	listener.AddIgnored(`\.lck`)
	listener.AddIgnored(`\.comp`)
	listener.AddIgnored(regexp.QuoteMeta(config.Cache_File_Name))

	// Start listening threads
	go finishFile(addition_channel)
	cache_err := listener.LoadCache()
	if cache_err != nil {
		util.LogError("Error loading listener cache:", cache_err.Error())
	}
	go listener.Listen(addition_channel)

	// Register request handling functions
	http.HandleFunc("/send.go", sendHandler)
	http.HandleFunc("/disk_add.go", diskWriteHandler)
	http.HandleFunc("/poll.go", pollHandler)
	http.HandleFunc("/editor.go", config.EditConfigInterface)
	http.HandleFunc("/edit_config.go", config.EditConfig)
	http.HandleFunc("/", errorHandler)

	// Setup server with correct HTTP protocol
	var serv net.Listener
	var serv_err error
	address := fmt.Sprintf(":%s", config.Server_Port)
	if config.Protocol() == "https" {
		serv, serv_err = util.AsyncListenAndServeTLS(address, config.Server_SSL_Cert, config.Server_SSL_Key)
	} else if config.Protocol() == "http" {
		serv, serv_err = util.AsyncListenAndServe(address)
	} else {
		util.LogError(fmt.Sprintf("Protocol type %s is not valid"), config.Protocol())
	}
	if serv_err != nil {
		util.LogError(serv_err.Error())
	}

	// Enter mainloop to check for config changes
	util.LogDebug("RECEIVER Ready")
	for {
		checkReload(serv)
		time.Sleep(1 * time.Second)
	}
}

// errorHandler is called when any page that is not a registered API method is requested.
func errorHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, http.StatusNotFound)
}

// getPartLocation parses the "location" part Header and returns the first and last byte from the part.
// Location format: "start_byte:end_byte"
func getPartLocation(location string) (int64, int64) {
	split_header := strings.Split(location, ":")
	start, start_err := strconv.ParseInt(split_header[0], 10, 64)
	end, end_err := strconv.ParseInt(split_header[1], 10, 64)
	if start_err != nil {
		util.LogError(start_err.Error())
	}
	if end_err != nil {
		util.LogError(end_err.Error())
	}
	return start, end
}

// sendHandler receives a multipart file from the sender via PUT request.
// It is responsible for verifying the md5 of each part in the file, replicating
// its directory structure as it was on the sender, and writing the file to disk.
func sendHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	host_name := util.GetHostname(r)
	compression := false
	if r.Header.Get("Content-Encoding") == "gzip" {
		compression = true
	}
	boundary := r.Header.Get("Boundary")
	multipart_reader := multipart.NewReader(r.Body, boundary)
	for {
		next_part, end_of_file := multipart_reader.NextPart()
		if end_of_file != nil { // Reached end of multipart file
			break
		}
		handle_success := handlePart(next_part, boundary, host_name, compression)
		if !handle_success {
			fmt.Fprint(w, http.StatusPartialContent) // respond with a 206
			return
		}
	}
	fmt.Fprint(w, http.StatusOK)
}

// handlePart is called for each part in the multipart file that is sent to sendHandler.
// It reads from the Part stream and writes the part to a file while calculating the md5.
// If the file is bad, it will be reacquired and passed back into sendHandler.
func handlePart(part *multipart.Part, boundary string, host_name string, compressed bool) bool {
	// Gather data about part from headers
	part_path := util.JoinPath(config.Staging_Directory, host_name, part.Header.Get("name"))
	write_path := part_path + ".tmp"
	part_start, _ := getPartLocation(part.Header.Get("location"))
	part_md5 := util.NewStreamMD5()
	// If the file which this part belongs to does not already exist, create a new empty file and companion file.
	_, err := os.Open(write_path)
	if os.IsNotExist(err) {
		size_header := part.Header.Get("total_size")
		total_size, parse_err := strconv.ParseInt(size_header, 10, 64)
		if parse_err != nil {
			util.LogError(fmt.Sprintf("Could not parse %s to int64: %s", size_header, parse_err.Error()))
		}
		createEmptyFile(part_path, total_size, host_name)
	}
	// Start reading and iterating over the part
	part_fi, open_err := os.OpenFile(write_path, os.O_WRONLY, 0600)
	if open_err != nil {
		util.LogError("Could not open file while trying to write part:", open_err.Error())
	}
	part_fi.Seek(part_start, 0)
	part_bytes := make([]byte, part_md5.BlockSize)
	var gzip_reader *gzip.Reader
	if compressed {
		var gzip_err error
		gzip_reader, gzip_err = gzip.NewReader(part)
		if gzip_err != nil {
			util.LogError("Could not create new gzip reader while parsing sent part:", gzip_err.Error())
		}
	}
	for {
		var err error
		var bytes_read int
		// The number of bytes read can often be less than the size of the passed buffer.
		if compressed {
			bytes_read, err = gzip_reader.Read(part_bytes)
		} else {
			bytes_read, err = part.Read(part_bytes)
		}
		part_md5.Update(part_bytes[0:bytes_read])
		part_fi.Write(part_bytes[0:bytes_read])
		if err != nil {
			break
		}
	}
	part_fi.Close()
	// Validate part
	if (part.Header.Get("md5") != part_md5.SumString()) || debugBinFail() { // If debug is enabled, drop bin 1/5th of the time
		// Validation failed, request this specific part again using part size
		util.LogError(fmt.Sprintf("Bad part of %s from bytes %s", part_path, part.Header.Get("location")))
		return false
	}
	// Update the companion file of the part, and check if the whole file is done
	add_err := util.AddPartToCompanion(part_path, part.Header.Get("md5"), part.Header.Get("location"), part.Header.Get("file_md5"), part.Header.Get("last_file"))
	if add_err != nil {
		util.LogError("Failed to add part to companion:", add_err.Error())
	}
	if isFileComplete(part_path) {
		// Finish file by updating the modtime and removing the .tmp extension.
		rename_err := os.Rename(write_path, part_path)
		if rename_err != nil {
			util.LogError(rename_err.Error())
			panic("Fatal error")
		}
		os.Chtimes(part_path, time.Now(), time.Now()) // Update mtime so that listener will pick up the file
		util.LogDebug("RECEIVER File Done:", part_path)
	}
	return true
}

func debugBinFail() bool {
	if DEBUG {
		if rand.Intn(5) == 3 {
			return true
		}
	}
	return false
}

// isFileComplete decodes the companion file of a given path and determines whether the file is complete.
// It sums the number of bytes in each part in the companion file. If the sum equals the total file size,
// the function returns true.
func isFileComplete(path string) bool {
	is_done := false
	decoded_companion, comp_err := util.DecodeCompanion(path)
	if comp_err != nil {
		util.LogError(fmt.Sprintf("Error decoding companion file at %s: %s", path, comp_err.Error()))
	}
	companion_size := int64(0)
	for _, element := range decoded_companion.CurrentParts {
		part_locations := strings.Split(element, ";")[1]
		start, end := getPartLocation(part_locations)
		part_size := end - start
		companion_size += part_size
	}
	if companion_size == decoded_companion.TotalSize {
		is_done = true
	}
	return is_done
}

// createNewFile is called when a multipart part is encountered and the file doesn't exist on the receiver yet.
// It fills a file with null bytes so that the created file is the same size as the complete file will be.
// It requires the hostname to create the companion file
func createEmptyFile(path string, size int64, host_name string) {
	os.MkdirAll(filepath.Dir(path), os.ModePerm)
	_, comp_err := util.NewCompanion(path, size, host_name)
	if comp_err != nil {
		util.LogError("Could not create new companion:", comp_err.Error())
	}
	fi, create_err := os.Create(path + ".tmp")
	if create_err != nil {
		util.LogError(fmt.Sprintf("Couldn't create empty file at %s.tmp with size %d: %s", path, size, create_err.Error()))
	}
	fi.Truncate(size)
	fi.Close()
}

// getStorePath returns the path that the receiver should use to store a file.
// Given parameters full_path and watch_directory, it will remove watch directory from the full path.
// This function differs from getStorePath() on the sender because the receiver watches its containing directory.
func getStorePath(full_path string, watch_directory string) string {
	store_path := strings.Replace(full_path, watch_directory+util.Sep, "", 1)
	return store_path
}

// finishFile blocks while listening for any additions on addition_channel.
// Once a file that isn't a temp file is found, it renames and cleans up the file,
// making sure to rename the files for a specific tag in order.
func finishFile(addition_channel chan string) {
	for {
		moved_map := make(map[string]int) // Create a map to count how many times we've tried to finish each file.
		new_file := <-addition_channel
		if strings.HasPrefix(new_file, config.Output_Directory) {
			continue // Don't process files that have already been finished.
		}
		// Acquire the mutex while working with new files so that the cache will re-detect unprocessed files in the event of a crash.
		go func() { // Create inline function so we can defer the release of the mutex lock.
			finalize_mutex.Lock()
			defer finalize_mutex.Unlock()
			// Recreate the staging directory path so the companion can be taken care of.
			host_name := strings.Split(strings.SplitN(new_file, config.Staging_Directory+util.Sep, 2)[1], util.Sep)[0]
			// Get file size & md5
			info, stat_err := os.Stat(new_file)
			if stat_err != nil {
				util.LogError("Couldn't stat file: ", stat_err.Error())
			}
			companion, comp_err := util.DecodeCompanion(new_file)
			if comp_err != nil {
				util.LogError(fmt.Sprintf("Error decoding companion file at %s: %s", new_file, comp_err.Error()))
				return
			}
			// If the companion has records of a last file, check to make sure we've already renamed it.
			if len(companion.Last_File) > 1 {
				time_multiplier := 1
				stored_multiplier, ok := moved_map[new_file]
				if ok {
					time_multiplier = stored_multiplier
					if time_multiplier > LOG_SEARCH_WIDTH {
						time_multiplier = LOG_SEARCH_WIDTH
					}
				}
				start_time := time.Now().Second() - 3600*24*time_multiplier // This is a hack, we don't know when the last file began sending, so look back one day, for every failure, increase search width.
				// We'll look up to a month in either direction for the file, but we won't be happy about it.
				if isFileMoved(util.JoinPath(companion.Last_File), companion.SenderName, int64(start_time)) == 0 {
					moved_map[new_file] += 1
					addition_channel <- new_file
					return
				}
			}
			// Validate the whole file MD5.
			file_md5 := companion.File_MD5
			computed_md5, md5_err := util.FileMD5(new_file)
			if md5_err != nil {
				util.LogError(fmt.Sprintf("Error while calculating MD5 of %s: %s", new_file, md5_err))
			}
			replace_portion := config.Staging_Directory + util.Sep
			log_name := strings.Replace(new_file, replace_portion, "", 1)
			if computed_md5 != file_md5 {
				// Welp, somehow the whole file didn't validate. Let's ask for it again.
				util.LogError(fmt.Sprintf("File %s failed whole file validation, asking sender to resend", new_file))
				log_map_lock.Lock()
				log_map[log_name] = false
				log_map_lock.Unlock()
			}
			// Finally, rename and clean up the file
			root_pattern := fmt.Sprintf("%s/%s/[^/]*/", config.Staging_Directory, host_name)

			compiled_pattern := regexp.MustCompile(root_pattern)
			remove_part := compiled_pattern.FindString(new_file)
			removed_root := strings.Replace(new_file, remove_part, "", 1)
			final_path := util.JoinPath(config.Output_Directory, host_name, removed_root)
			os.MkdirAll(filepath.Dir(final_path), os.ModePerm) // Make containing directories for the file.
			rename_err := os.Rename(new_file, final_path)
			if rename_err != nil {
				util.LogError(fmt.Sprintf("Couldn't rename file while moving %s to output directory: %s", new_file, rename_err.Error()))
			}
			// Remove companion file
			companion_name := new_file + ".comp"
			remove_err := os.Remove(companion_name)
			if remove_err != nil {
				util.LogError(fmt.Sprintf("Couldn't remove companion file %s: %s", companion_name, remove_err.Error()))
			}
			log_map_lock.Lock()
			delete(moved_map, new_file)
			log_map[log_name] = true
			if len(log_map) > 10000 {
				// Dump the log map when it gets too large.
				log_map = make(map[string]bool)
			}
			receiver_log.LogReceive(log_name, file_md5, info.Size(), host_name)
			log_map_lock.Unlock()
		}()
	}
}

// diskWriteHandler is called by the sender to let the receiver know when it has written a file to disk.
func diskWriteHandler(w http.ResponseWriter, r *http.Request) {
	file_path := r.FormValue("name")
	md5 := r.FormValue("md5")
	size, parse_err := strconv.ParseInt(r.FormValue("size"), 10, 64)
	if parse_err != nil {
		util.LogError(fmt.Sprintf("Couldn't parse int64 from %s: %s", r.FormValue("size"), parse_err.Error()))
	}
	disk_log.LogDisk(file_path, md5, size)
	fmt.Fprint(w, http.StatusOK)
}

// pollHandler is called by the sender to ask the receiver which files have been fully written and validated.
func pollHandler(w http.ResponseWriter, r *http.Request) {
	// Get host name of request
	host_name := util.GetHostname(r)
	files := strings.Split(r.FormValue("files"), ",")
	response_map := make(map[string]int, len(files))
	for _, file_data := range files {
		split_data := strings.Split(file_data, ";")
		start_time, _ := strconv.ParseInt(split_data[1], 10, 64)
		response_map[split_data[0]] = isFileMoved(split_data[0], host_name, start_time)
	}
	json_response, _ := json.Marshal(response_map)
	w.Write(json_response)
}

// isFileMoved returns the completion status of the file. Possible states:
//  0 - If the file hasn't been processed yet.
//  1 - If the file has sent successfully.
// -1 - If the file sending failed.
func isFileMoved(path string, host_name string, start_time int64) int {
	log_map_path := util.JoinPath(host_name, path)
	log_map_lock.Lock()
	success, verified := log_map[log_map_path]
	log_map_lock.Unlock()
	if verified && success {
		return 1
	} else if verified && !success {
		return -1
	}
	// Get the path of this file in the
	temp_path := config.Staging_Directory + util.Sep + path
	companion_path := temp_path + ".comp"
	_, file_exists := os.Stat(temp_path + ".tmp")
	_, comp_exists := os.Stat(companion_path)
	if os.IsExist(file_exists) || os.IsExist(comp_exists) {
		// The file is still in the staging area, so no need to check logs
		return 0
	}
	// Get paths of logs to search for the file
	logs := make([]string, LOG_SEARCH_WIDTH)
	for index, _ := range logs {
		timestamp := time.Unix(start_time+int64((3600*24*index)), 0) // Find the next day of logs by adding X days to the timestamp
		logs[index] = receiver_log.GetLogPath(timestamp, host_name)
	}
	// Actually search through the log files for the file info
	path_bytes := []byte(path + ":")
	for _, log_path := range logs {
		log_handle, open_err := os.Open(log_path)
		if open_err != nil {
			// The log probably just doesn't exist yet because we're looking in the future, no big deal.
			continue
		}
		// Use a byte scanner to find the path bytes in the log file. Fancy.
		log_scanner := bufio.NewScanner(log_handle)
		for log_scanner.Scan() {
			if bytes.Contains(log_scanner.Bytes(), path_bytes) {
				log_handle.Close()
				return 1
			}
		}
		log_handle.Close()
	}
	return 0
}

// onFinish will prevent the cache from writing newer timestamps to disk while
// files from the addition channel are still being processed by finishFile.
// In the event of a crash, the listener will pick up unfinished files again.
func onFinish() {
	finalize_mutex.Lock()
	finalize_mutex.Unlock()
}

// checkReload checks if the config has been changed. If the config has changed,
// it is determined whether the change can be processed without restarting the program.
// If it can, the new values are propagated. If it can't, the program is restarted.
func checkReload(server net.Listener) {
	if config.ShouldReload() {
		// Update in-memory config
		old_config := config
		temp_config, parse_err := util.ParseConfig(config.FileName())
		if parse_err != nil {
			util.LogError("Failed to parse config file: ", config.FileName(), " (changes not accepted)", parse_err.Error())
			config.Reloaded()
			return
		}
		config = temp_config
		if config.StaticDiff(old_config) {
			util.LogError("Static config value(s) changed, restarting...")
			server.Close()
			util.Restart()
		} else {
			// If there were dynamic values, this is where they would be reloaded.
		}
		config.Reloaded()
	}
}
