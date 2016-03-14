package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
)

// ConfirmNone is the indicator that a file has not been confirmed.
const ConfirmNone = 0

// ConfirmFailed is the indicator that file confirmation failed.
const ConfirmFailed = 1

// ConfirmPassed is the indicator that file confirmation succeeded.
const ConfirmPassed = 2

// Poller continually asks the receiver to confirm that files allocated as -1 were successfully
// assembled. Broker constantly feeds Poller new files while allocating bins.
type Poller struct {
	Files  map[string]*pollFile
	client *http.Client
	conf   *SenderConf
}

type pollFile struct {
	file   SendFile
	time   int64
	nRetry int
}

func (f *pollFile) GetSortFile() SortFile {
	return f.file.GetSortFile()
}

// NewPoller creates a new Poller instance with all default variables instantiated.
func NewPoller(conf *SenderConf) *Poller {
	p := &Poller{}
	p.conf = conf
	p.Files = make(map[string]*pollFile)
	var err error
	p.client, err = httputils.GetClient(conf.TLSCert, conf.TLSKey)
	if err != nil {
		logging.Error(err.Error())
	}
	return p
}

// Start runs the poller in a loop and operates on the input channels.
func (poller *Poller) Start(inChan chan SendFile, failChan chan SendFile, doneChan []chan DoneFile) {
	var t int64
	pause := time.Millisecond * 100
	for {
		time.Sleep(pause) // To keep from thrashing
		for {
			select {
			case file := <-inChan:
				poller.addFile(file)
				continue
			default:
				break
			}
			break
		}

		if t > 0 && int(time.Now().Unix()-t) < poller.conf.PollInterval {
			continue
		}
		if len(poller.Files) < 1 {
			continue
		}
		files := poller.buildList(failChan)
		if len(files) < 1 {
			continue
		}
		err := poller.poll(files, failChan, doneChan)
		if err != nil {
			t = time.Now().Unix()
			continue
		}
	}
}

func (poller *Poller) addFile(file SendFile) {
	_, exists := poller.Files[file.GetRelPath()]
	if !exists {
		logging.Debug("POLLER Added:", file.GetRelPath())
		pfile := &pollFile{file, time.Now().Unix(), 0}
		poller.Files[file.GetRelPath()] = pfile
	}
}

func (poller *Poller) removeFile(file *pollFile) {
	delete(poller.Files, file.file.GetRelPath())
}

func (poller *Poller) buildList(failChan chan SendFile) []*ConfirmFile {
	list := []*ConfirmFile{}
	for _, pf := range poller.Files {
		if poller.conf.MaxRetries > 0 && pf.nRetry > poller.conf.MaxRetries {
			logging.Debug("POLLER Rejected:", pf.file.GetRelPath())
			// Send the file again
			select {
			case failChan <- pf.file:
				poller.removeFile(pf)
			default:
				break
			}
			continue
		}
		if pf.time < (time.Now().Unix() - int64(poller.conf.PollDelay)) {
			logging.Debug("POLLER Request:", pf.file.GetRelPath())
			cf := &ConfirmFile{pf.file.GetRelPath(), pf.time}
			list = append(list, cf)
			pf.nRetry++
		}
	}
	return list
}

func (poller *Poller) poll(files []*ConfirmFile, failChan chan SendFile, doneChan []chan DoneFile) error {
	payloadJSON, err := json.Marshal(files)
	if err != nil {
		return err
	}
	logging.Debug("POLLING:", len(files))
	url := fmt.Sprintf("%s://%s/validate", poller.conf.Protocol(), poller.conf.TargetHost)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadJSON))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add(HeaderSourceName, poller.conf.SourceName)
	if err != nil {
		return err
	}
	resp, err := poller.client.Do(req)
	if err != nil {
		return err
	}
	fileMap := make(map[string]int)
	jsonDecoder := json.NewDecoder(resp.Body)
	err = jsonDecoder.Decode(&fileMap)
	if err != nil {
		return err
	}
	for path, code := range fileMap {
		pf := poller.Files[path]
		switch code {
		case ConfirmPassed:
			logging.Debug("POLLER Confirmation:", path)
			for _, dc := range doneChan {
				dc <- pf.file
			}
		case ConfirmFailed:
			logging.Error("POLLER Failed Confirmation:", path)
			failChan <- pf.file
		}
		poller.removeFile(pf)
	}
	return nil
}
