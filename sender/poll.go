package sender

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/util"
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
	Path        string
	Start_Time  int64
	add_time    int64
	retry_count int
}

// NewPoller creates a new Poller instance with all default variables instantiated.
func NewPoller(cache *Cache) *Poller {
	new_poller := &Poller{}
	new_poller.validation_map = make(map[string]*PollData)
	new_poller.cache = cache
	new_poller.map_mutex = sync.Mutex{}
	var client_err error
	new_poller.client, client_err = util.GetTLSClient(config.Client_SSL_Cert, config.Client_SSL_Key, config.TLS)
	if client_err != nil {
		util.LogError(client_err.Error())
	}
	return new_poller
}

// addFile safely adds a new file to the Poller's list of files to verify. It requires the start
// timestamp of the file, so that the receiver knows where in the logs to look for the file
func (poller *Poller) addFile(path string, start_time int64) {
	util.LogDebug("POLLER Added:", path)
	store_path := getStorePath(path, config.Input_Directory)
	poller.map_mutex.Lock()
	defer poller.map_mutex.Unlock()
	_, exists := poller.validation_map[store_path]
	if exists {
		return
	} else {
		new_data := &PollData{store_path, start_time, util.GetTimestamp(), 0}
		poller.validation_map[store_path] = new_data
	}
}

func (poller *Poller) getFiles() []*PollData {
	list := []*PollData{}
	poller.map_mutex.Lock()
	defer poller.map_mutex.Unlock()
	for _, pd := range poller.validation_map {
		pd.retry_count += 1
		if pd.retry_count > 100 {
			// Send the file again
			delete(poller.validation_map, pd.Path)
			poller.cache.resendFile(getWholePath(pd.Path))
			continue
		}
		if pd.add_time < (util.GetTimestamp() - int64(config.Poll_Delay)) {
			util.LogDebug("POLLER Request:", pd.Path)
			list = append(list, pd)
		}
	}
	return list
}

// poll is the blocking mainloop of Poller. It constantly sends out
// HTTP requests to verify files in its list. When a file has been
// verified, it is removed from the cache.
func (poller *Poller) poll() {
	for {
		time.Sleep(time.Second * time.Duration(config.Poll_Interval))
		if len(poller.validation_map) < 1 {
			continue
		}
		payload := poller.getFiles()
		if len(payload) < 1 {
			continue
		}
		payload_json, err := json.Marshal(payload)
		if err != nil {
			util.LogError("Failed JSON encoding")
			continue
		}
		req_url := fmt.Sprintf("%s://%s/validate", config.Protocol(), config.Receiver_Address)
		req, err := http.NewRequest("POST", req_url, bytes.NewBuffer(payload_json))
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("X-STS-SenderName", config.Sender_Name)
		if err != nil {
			util.LogError("Failed to generate HTTP request object:", err.Error())
		}
		resp, err := poller.client.Do(req)
		if err != nil {
			// Receiver is probably down
			util.LogError("Poll request failed, receiver is probably down:", err.Error())
			continue
		}
		err = poller.unpackResponse(resp)
		if err != nil {
			util.LogError("Error unpacking response to polling request:", err.Error())
		}
	}
}

func (poller *Poller) unpackResponse(response *http.Response) error {
	response_map := make(map[string]int)
	json_decoder := json.NewDecoder(response.Body)
	decode_err := json_decoder.Decode(&response_map)
	if decode_err != nil {
		return decode_err
	}
	// Update the validation_map with response info
	for path, _ := range response_map {
		// Get info you need to address the file in the cache
		whole_path := getWholePath(path)
		tag_data := getTag(poller.cache, whole_path)
		stat, stat_err := os.Stat(whole_path)
		if stat_err != nil {
			util.LogError(fmt.Sprintf("Failed to stat file %s during cleanup: %s", whole_path, stat_err.Error()))
			continue
		}
		if response_map[path] == 1 {
			util.LogDebug("POLLER Confirmation:", path)
			// Delete the file if the tag says we should.
			if tag_data.Delete_On_Verify {
				remove_err := os.Remove(whole_path)
				if remove_err != nil {
					util.LogError(fmt.Sprintf("Failed to remove %s after confirmation: %s", whole_path, remove_err.Error()))
				} else {
					util.LogDebug("POLLER Delete:", path)
				}
			}
			// Make sure we don't remove files in the middle of allocation.
			poller.cache.allocation_lock.Lock()
			poller.cache.removeFile(whole_path)
			poller.cache.allocation_lock.Unlock()
			// Lock the log map while removing items
			poller.map_mutex.Lock()
			poller.validation_map[path] = nil
			delete(poller.validation_map, path)
			poller.map_mutex.Unlock()
		} else if response_map[path] == -1 {
			// File failed whole-file validation, set for re-allocation.
			util.LogError("Failed to send (resending...):", path)
			poller.cache.updateFileAlloc(whole_path, 0, tag_data, stat)
			poller.cache.listener.WriteCache()
		}
	}
	return nil
}
