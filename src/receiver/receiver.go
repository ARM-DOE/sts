package receiver

import (
    "compress/gzip"
    "fmt"
    "io"
    "io/ioutil"
    "math/rand"
    "mime/multipart"
    "net"
    "net/http"
    "os"
    path_util "path"
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

var config util.Config
var client http.Client

var error_log util.Logger
var receiver_log util.Logger
var disk_log util.Logger

const DEBUG = true

// Main is the entry point of the webserver. It is responsible for registering HTTP
// handlers, parsing the config file, starting the file listener, and beginning the request serving loop.
func Main(config_file string) {
    // Setup mutex locks and parse config into global variable.
    finalize_mutex = sync.Mutex{}
    util.CompanionLock = sync.Mutex{}
    var parse_err error
    config, parse_err = util.ParseConfig(config_file)
    if parse_err != nil {
        fmt.Println("Couldn't parse config", parse_err.Error())
    }
    // Setup loggers
    receiver_log = util.NewLogger(config.Logs_Directory, util.LOGGING_RECEIVE)
    error_log = util.NewLogger(config.Logs_Directory, util.LOGGING_ERROR)
    disk_log = util.NewLogger(config.Logs_Directory, util.LOGGING_DISK)
    // Create HTTP client
    var client_err error
    client, client_err = util.GetTLSClient(config.Client_SSL_Cert, config.Client_SSL_Key)
    if client_err != nil {
        error_log.LogError(client_err.Error())
    }
    // Setup listener and add ignore patterns.
    addition_channel := make(chan string, 1)
    listener := util.NewListener(config.Cache_File_Name, error_log, config.Staging_Directory, config.Output_Directory)
    listener.SetOnFinish(onFinish)
    listener.AddIgnored(`\.tmp`)
    listener.AddIgnored(`\.comp`)
    listener.AddIgnored(regexp.QuoteMeta(config.Cache_File_Name))
    // Start listening threads
    go finishFile(addition_channel)
    cache_err := listener.LoadCache()
    if cache_err != nil {
        error_log.LogError("Error loading listener cache:", cache_err.Error())
    }
    go listener.Listen(addition_channel)
    // Register request handling functions
    http.HandleFunc("/send.go", sendHandler)
    http.HandleFunc("/disk_add.go", diskWriteHandler)
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
        error_log.LogError(fmt.Sprintf("Protocol type %s is not valid"), config.Protocol())
    }
    if serv_err != nil {
        error_log.LogError(serv_err.Error())
    }
    // Enter mainloop to check for config changes
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
        error_log.LogError(start_err.Error())
    }
    if end_err != nil {
        error_log.LogError(end_err.Error())
    }
    return start, end
}

// sendHandler receives a multipart file from the sender via PUT request.
// It is responsible for verifying the md5 of each part in the file, replicating
// its directory structure as it was on the sender, and writing the file to disk.
func sendHandler(w http.ResponseWriter, r *http.Request) {
    defer r.Body.Close()
    ip, _, _ := net.SplitHostPort(r.RemoteAddr)
    host_name := util.GetHostname(ip)
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
    part_path := util.JoinPath(config.Staging_Directory, part.Header.Get("name"))
    write_path := part_path + ".tmp"
    part_start, _ := getPartLocation(part.Header.Get("location"))
    part_md5 := util.NewStreamMD5()
    // If the file which this part belongs to does not already exist, create a new empty file and companion file.
    _, err := os.Open(write_path)
    if os.IsNotExist(err) {
        size_header := part.Header.Get("total_size")
        total_size, parse_err := strconv.ParseInt(size_header, 10, 64)
        if parse_err != nil {
            error_log.LogError(fmt.Sprintf("Could not parse %s to int64: %s", size_header, parse_err.Error()))
        }
        createEmptyFile(part_path, total_size, host_name)
    }
    // Start reading and iterating over the part
    part_fi, open_err := os.OpenFile(write_path, os.O_WRONLY, 0600)
    if open_err != nil {
        error_log.LogError("Could not open file while trying to write part:", open_err.Error())
    }
    part_fi.Seek(part_start, 0)
    part_bytes := make([]byte, part_md5.BlockSize)
    var gzip_reader *gzip.Reader
    if compressed {
        var gzip_err error
        gzip_reader, gzip_err = gzip.NewReader(part)
        if gzip_err != nil {
            error_log.LogError("Could not create new gzip reader while parsing sent part:", gzip_err.Error())
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
        error_log.LogError(fmt.Sprintf("Bad part of %s from bytes %s", part_path, part.Header.Get("location")))
        return false
    }
    // Update the companion file of the part, and check if the whole file is done
    add_err := util.AddPartToCompanion(part_path, part.Header.Get("md5"), part.Header.Get("location"), part.Header.Get("file_md5"))
    if add_err != nil {
        error_log.LogError("Failed to add part to companion:", add_err.Error())
    }
    if isFileComplete(part_path) {
        // Finish file by moving it to the final directory and removing the .tmp extension.
        tmp := util.JoinPath(config.Output_Directory, host_name, part_path)
        rename_path := getStorePath(tmp, config.Staging_Directory)
        os.MkdirAll(filepath.Dir(rename_path), os.ModePerm) // Make containing directories for the file.
        rename_err := os.Rename(write_path, rename_path)
        if rename_err != nil {
            error_log.LogError(rename_err.Error())
            panic("Fatal error")
        }
        os.Chtimes(rename_path, time.Now(), time.Now()) // Update mtime so that listener will pick up the file
        fmt.Println("Fully assembled ", part_path)
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
        error_log.LogError(fmt.Sprintf("Error decoding companion file at %s: %s", path, comp_err.Error()))
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
        error_log.LogError("Could not create new companion:", comp_err.Error())
    }
    fi, create_err := os.Create(path + ".tmp")
    if create_err != nil {
        error_log.LogError(fmt.Sprintf("Couldn't create empty file at %s.tmp with size %d: %s", path, size, create_err.Error()))
    }
    fi.Truncate(size)
    fi.Close()
}

// getStorePath returns the path that the receiver should use to store a file.
// Given parameters full_path and watch_directory, it will remove watch directory from the full path.
// This function differs from getStorePath() on the sender because the receiver watches its containing directory.
func getStorePath(full_path string, watch_directory string) string {
    store_path := strings.Replace(full_path, watch_directory+string(os.PathSeparator), "", 1)
    return store_path
}

// removeFromCache removes the specified file from the cache on the Sender.
// It loops until the Sender confirms that the file has been removed.
func removeFromCache(path string) {
    request_complete := false
    companion, comp_err := util.DecodeCompanion(path)
    if comp_err != nil {
        error_log.LogError(fmt.Sprintf("Error decoding companion file at %s: %s", path, comp_err.Error()))
    }
    host_name := companion.SenderName
    post_url := fmt.Sprintf("%s://%s:%s/remove.go?name=%s", config.Protocol(), host_name, config.Sender_Server_Port, getStorePath(path, config.Staging_Directory))
    for !request_complete {
        request, req_err := http.NewRequest("POST", post_url, nil)
        if req_err != nil {
            error_log.LogError(fmt.Sprintf("Could not create HTTP request object with URL %s: %s", post_url, req_err.Error()))
        }
        response, resp_err := client.Do(request)
        if resp_err != nil {
            error_log.LogError("Request to remove from cache failed:", resp_err.Error())
            time.Sleep(5 * time.Second)
            continue
        } else {
            request_complete = true
        }
        // Discard request response and close request
        io.Copy(ioutil.Discard, response.Body)
        response.Body.Close()
        response.Close = true
    }
}

// finishFile blocks while listening for any additions on addition_channel.
// Once a file that isn't a temp file is found, it removes the file from the sender's cache.
func finishFile(addition_channel chan string) {
    for {
        new_file := <-addition_channel
        // Acquire the mutex while working with new files so that the cache will re-detect unprocessed files in the event of a crash.
        finalize_mutex.Lock()
        func() { // Create inline function so we can defer the release of the mutex lock.
            defer finalize_mutex.Unlock()
            // Recreate the staging directory path so the companion can be taken care of.
            host_name := strings.Split(getStorePath(new_file, config.Output_Directory), string(os.PathSeparator))[0]
            staged_dir := util.JoinPath(config.Staging_Directory, getStorePath(new_file, util.JoinPath(config.Output_Directory, host_name)))
            // Get file size & md5
            info, stat_err := os.Stat(new_file)
            if stat_err != nil {
                error_log.LogError("Couldn't stat file: ", stat_err.Error())
            }
            companion, comp_err := util.DecodeCompanion(staged_dir)
            if comp_err != nil {
                error_log.LogError(fmt.Sprintf("Error decoding companion file at %s: %s", staged_dir, comp_err.Error()))
                return
            }
            file_md5 := companion.File_MD5
            // Finally, clean up the file
            removeFromCache(staged_dir)
            receiver_log.LogReceive(path_util.Base(new_file), file_md5, info.Size(), host_name)
            os.Remove(staged_dir + ".comp")
        }()
    }
}

// diskWriteHandler is called by the sender to let the receiver know when it has written a file to disk.
func diskWriteHandler(w http.ResponseWriter, r *http.Request) {
    file_path := r.FormValue("name")
    md5 := r.FormValue("md5")
    size, parse_err := strconv.ParseInt(r.FormValue("size"), 10, 64)
    if parse_err != nil {
        error_log.LogError(fmt.Sprintf("Couldn't parse int64 from %s: %s", r.FormValue("size"), parse_err.Error()))
    }
    disk_log.LogDisk(file_path, md5, size)
    fmt.Fprint(w, http.StatusOK)
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
            error_log.LogError("Couldn't parse config file, changes not accepted:", parse_err.Error())
            config.Reloaded()
            return
        }
        config = temp_config
        if config.StaticDiff(old_config) {
            error_log.LogError("Static config value(s) changed, restarting...")
            server.Close()
            util.Restart()
        } else {
            // If there were dynamic values, this is where they would be reloaded.
        }
        config.Reloaded()
    }
}
