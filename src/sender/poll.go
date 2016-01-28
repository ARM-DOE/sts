package sender

import (
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "os"
    "strings"
    "sync"
    "time"
    "util"
)

// Poller continually asks the receiver to confirm that files allocated as -1 were successfully
// assembled. Cache constantly feeds Poller new files while allocating bins.
type Poller struct {
    validation_map map[string]*PollData
    map_mutex      sync.Mutex
    client         http.Client
    cache          *Cache
}

type PollData struct {
    start_time  int64
    retry_count int
}

// NewPoller creates a new Poller instance with all default variables instantiated.
func NewPoller(cache *Cache) *Poller {
    new_poller := &Poller{}
    new_poller.validation_map = make(map[string]*PollData)
    new_poller.cache = cache
    new_poller.map_mutex = sync.Mutex{}
    var client_err error
    new_poller.client, client_err = util.GetTLSClient(config.Client_SSL_Cert, config.Client_SSL_Key)
    if client_err != nil {
        error_log.LogError(client_err.Error())
    }
    return new_poller
}

// addFile safely adds a new file to the Poller's list of files to verify. It requires the start
// timestamp of the file, so that the receiver knows where in the logs to look for the file
func (poller *Poller) addFile(path string, start_time int64) {
    store_path := getStorePath(path, config.Input_Directory)
    poller.map_mutex.Lock()
    defer poller.map_mutex.Unlock()
    _, exists := poller.validation_map[store_path]
    if exists {
        return
    } else {
        new_data := &PollData{start_time, 0}
        poller.validation_map[store_path] = new_data
    }
}

// poll is the blocking mainloop of Poller. It constantly sends out
// HTTP requests to verify files in its list. When a file has been
// verified, it is removed from the cache.
func (poller *Poller) poll() {
    for {
        if len(poller.validation_map) < 1 {
            time.Sleep(time.Second * 1)
            continue
        }
        // Create the payload of the verification request
        payload := ""
        poller.map_mutex.Lock()
        for path, poll_data := range poller.validation_map {
            poll_data.retry_count += 1
            if poll_data.retry_count > 100 {
                // Send the file again
                delete(poller.validation_map, path)
                cache_path := getWholePath(path)
                poller.cache.resendFile(cache_path)
                continue
            }
            payload += fmt.Sprintf("%s;%d,", path, time.Duration(poll_data.start_time)/time.Second) // Convert timestamp to seconds
        }
        payload = strings.Trim(payload, ",")
        // Make sure that the payload is long enough to send. This check would activate if all files in the payload list were set to be resent
        if len(payload) <= 1 {
            time.Sleep(1)
            continue
        }
        poller.map_mutex.Unlock()
        request_url := fmt.Sprintf("%s://%s/poll.go?files=%s", config.Protocol(), config.Receiver_Address, url.QueryEscape(payload))
        new_request, request_err := http.NewRequest("POST", request_url, nil)
        if request_err != nil {
            error_log.LogError("Could not generate HTTP request object: ", request_err.Error())
        }
        response, response_err := poller.client.Do(new_request)
        if response_err != nil {
            // Receiver is probably down
            time.Sleep(time.Second * 5)
            error_log.LogError("Poll request failed, receiver is probably down:", response_err.Error())
            continue
        }
        response_map := make(map[string]bool)
        json_decoder := json.NewDecoder(response.Body)
        decode_err := json_decoder.Decode(&response_map)
        if decode_err != nil {
            // Json response may have been corrupted in transit, wait and try again
            error_log.LogError("Receiver returned malformed JSON in response to polling request, trying again:", decode_err.Error())
            time.Sleep(time.Second * 5)
            continue
        }
        // Update the validation_map with response info
        poller.map_mutex.Lock()
        for path, _ := range response_map {
            if response_map[path] {
                whole_path := getWholePath(path)
                tag_data := getTag(poller.cache, whole_path)
                if tag_data.Delete_On_Verify {
                    // Delete the file if the tag says we should.
                    remove_err := os.Remove(whole_path)
                    if remove_err != nil {
                        error_log.LogError(fmt.Sprintf("Couldn't remove %s file after send confirmation: %s", whole_path, remove_err.Error()))
                    }
                }
                poller.cache.removeFile(whole_path)
                poller.validation_map[path] = nil
                delete(poller.validation_map, path)
            }
        }
        poller.map_mutex.Unlock()
        // Sleep before next polling request
        time.Sleep(30 * time.Second)
    }
}
