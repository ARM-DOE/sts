package sender

import (
    "encoding/json"
    "fmt"
    "net/http"
    "strings"
    "sync"
    "time"
    "util"
)

// Poller continually asks the receiver to confirm that files allocated as -1 were successfully
// assembled. Cache constantly feeds Poller new files while allocating bins.
type Poller struct {
    validation_map map[string]bool
    map_mutex      sync.Mutex
    client         http.Client
    cache          *Cache
}

// NewPoller creates a new Poller instance with all default variables instantiated.
func NewPoller(cache *Cache) *Poller {
    new_poller := &Poller{}
    new_poller.validation_map = make(map[string]bool)
    new_poller.cache = cache
    new_poller.map_mutex = sync.Mutex{}
    var client_err error
    new_poller.client, client_err = util.GetTLSClient(config.Client_SSL_Cert, config.Client_SSL_Key)
    if client_err != nil {
        error_log.LogError(client_err.Error())
    }
    return new_poller
}

// addFile safely adds a new file to the Poller's list of files to verify.
func (poller *Poller) addFile(path string) {
    store_path := getStorePath(path, config.Input_Directory)
    poller.map_mutex.Lock()
    defer poller.map_mutex.Unlock()
    _, exists := poller.validation_map[store_path]
    if exists {
        return
    } else {
        poller.validation_map[store_path] = false
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
        for path, _ := range poller.validation_map {
            payload += path + ","
        }
        payload = strings.Trim(payload, ",")
        poller.map_mutex.Unlock()
        url := fmt.Sprintf("%s://%s/poll.go?files=%s", config.Protocol(), config.Receiver_Address, payload)
        new_request, request_err := http.NewRequest("POST", url, nil)
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
                poller.cache.removeFile(whole_path)
                delete(poller.validation_map, path)
            }
        }
        poller.map_mutex.Unlock()
    }
}
