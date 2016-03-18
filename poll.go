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

// Implements PollFile and DoneFile interfaces
type pollFile struct {
	file   SendFile
	time   time.Time
	nRetry int
}

func (f *pollFile) GetPath() string {
	return f.file.GetPath()
}

func (f *pollFile) GetRelPath() string {
	return f.file.GetRelPath()
}

func (f *pollFile) GetStarted() int64 {
	return f.time.UnixNano()
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
func (poller *Poller) Start(inChan chan []SendFile, failChan chan SendFile, doneChan []chan []DoneFile) {
	var t time.Time
	var err error
	fail := []SendFile{}
	poll := []PollFile{}
	done := make([][]DoneFile, len(doneChan))
	pause := time.Millisecond * 100
	for {
		time.Sleep(pause) // To keep from thrashing
		for {
			for len(fail) > 0 { // Send out any failed.
				select {
				case failChan <- fail[0]:
					fail = fail[1:]
					continue
				default:
					break
				}
				break
			}
			for i, dc := range doneChan { // Send out any done.
				if len(done[i]) > 0 {
					select {
					case dc <- done[i]:
						done[i] = []DoneFile{}
					default:
						break
					}
				}
			}
			select {
			case files := <-inChan: // Get incoming.
				for _, file := range files {
					poller.addFile(file)
				}
				continue
			default:
				break
			}
			break
		}
		if !t.IsZero() && time.Now().Sub(t) < poller.conf.PollInterval {
			continue
		}
		if len(poller.Files) < 1 {
			continue
		}
		var f []SendFile
		var d []DoneFile
		f, poll = poller.buildList()
		fail = append(fail, f...)
		if len(poll) < 1 {
			continue
		}
		f, d, err = poller.Poll(poll)
		fail = append(fail, f...)
		for i := range done {
			done[i] = append(done[i], d...)
		}
		if err != nil {
			t = time.Now()
			continue
		}
	}
}

func (poller *Poller) addFile(file SendFile) {
	_, exists := poller.Files[file.GetRelPath()]
	if !exists {
		logging.Debug("POLL Added:", file.GetRelPath())
		pfile := &pollFile{file, time.Now(), 0}
		poller.Files[file.GetRelPath()] = pfile
	}
}

func (poller *Poller) removeFile(file PollFile) {
	delete(poller.Files, file.GetRelPath())
}

func (poller *Poller) buildList() (fail []SendFile, poll []PollFile) {
	for _, pf := range poller.Files {
		if poller.conf.MaxRetries > 0 && pf.nRetry > poller.conf.MaxRetries {
			logging.Debug("POLL Rejected:", pf.file.GetRelPath())
			fail = append(fail, pf.file)
			continue
		}
		if time.Now().Sub(pf.time) > poller.conf.PollDelay {
			logging.Debug("POLL Request:", pf.file.GetRelPath())
			poll = append(poll, pf)
			pf.nRetry++
		}
	}
	return
}

// Poll attempts to confirm whether files have been properly received on target.
func (poller *Poller) Poll(files []PollFile) (fail []SendFile, done []DoneFile, err error) {
	fmap := make(map[string]PollFile)
	var cf []*ConfirmFile
	for _, f := range files {
		cf = append(cf, &ConfirmFile{f.GetRelPath(), int64(f.GetStarted() / 1e9)}) // GetStarted() returns in nanoseconds
		fmap[f.GetRelPath()] = f
	}
	payloadJSON, err := json.Marshal(cf)
	if err != nil {
		return
	}
	logging.Debug("POLLing for:", len(files))
	url := fmt.Sprintf("%s://%s/validate", poller.conf.Protocol(), poller.conf.TargetHost)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadJSON))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add(HeaderSourceName, poller.conf.SourceName)
	if err != nil {
		return
	}
	resp, err := poller.client.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("Poll request failed: %d", resp.StatusCode)
		return
	}
	fileMap := make(map[string]int)
	jsonDecoder := json.NewDecoder(resp.Body)
	err = jsonDecoder.Decode(&fileMap)
	if err != nil {
		return
	}
	for path, code := range fileMap {
		pf := fmap[path]
		df, ok := pf.(DoneFile)
		if !ok {
			_, ok = poller.Files[path]
			if !ok {
				continue
			}
			df = poller.Files[path].file.(DoneFile)
		}
		switch code {
		case ConfirmPassed:
			logging.Debug("POLL Confirmation:", path)
			done = append(done, df)
		case ConfirmFailed:
			logging.Error("POLL Failed Confirmation:", path)
			fail = append(fail, poller.Files[path].file.(SendFile))
		}
		poller.removeFile(pf)
	}
	return
}
