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

// Poller continually asks the receiver to confirm that files allocated as -1 were
// successfully assembled. Broker constantly feeds Poller new files while allocating
// bins.
type Poller struct {
	Files  map[string]*pollFile
	client *http.Client
	conf   *SenderConf
}

// PollerChan is the struct used to house the channels in support of flow
// through the poller.
type PollerChan struct {
	In   <-chan []SendFile
	Fail chan<- []SendFile
	Done []chan<- []DoneFile

	// Need a separate stop indicator rather than the closing of the "in" channel
	// because of the looping nature of this component.
	Stop <-chan bool
}

// Implements PollFile and DoneFile interfaces
type pollFile struct {
	file  SendFile
	time  time.Time
	nPoll int
}

func (f *pollFile) GetPath() string {
	return f.file.GetPath(false)
}

func (f *pollFile) GetRelPath() string {
	return f.file.GetRelPath()
}

func (f *pollFile) GetStarted() time.Time {
	return f.time
}

func (f *pollFile) GetSuccess() bool {
	return !f.time.IsZero()
}

func (f *pollFile) GetOrigFile() interface{} {
	return f.file
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

// Start runs the poller and goes until we get input on the stop channel.
func (poller *Poller) Start(ch *PollerChan) {
	var t time.Time
	var err error
	fail := []SendFile{}
	poll := []PollFile{}
	done := make([][]DoneFile, len(ch.Done))
	loop := 0
	pause := time.Millisecond * 100
	for {
		select {
		// We needed a way to communicate shutdown without closing the main input channel because the poller
		// operates on a loop with failed files.
		case s, ok := <-ch.Stop:
			if s || !ok {
				logging.Debug("POLL Got Shutdown Signal")
				ch.Stop = nil // Select will always drop to default (starting next iteration).
			}
		default:
			// If we got the shutdown signal that means everything is in our court.
			// So if we have nothing in the queue and nothing looped back for a resend, then we're done.
			if ch.Stop == nil && len(poller.Files) == 0 && loop == 0 {
				empty := true
				for i, dc := range ch.Done {
					if len(done[i]) > 0 {
						empty = false
						continue
					}
					close(dc)
				}
				if empty {
					close(ch.Fail)
					return
				}
			}
			break
		}
		time.Sleep(pause) // To keep from thrashing
		for {
			if len(fail) > 0 { // Send out any failed.
				logging.Debug("POLL Failed:", len(fail))
				if ch.Stop == nil {
					loop += len(fail) // Remember how many are in the loop so we know when to shutdown.
				}
				select {
				case ch.Fail <- fail:
					logging.Debug("POLL Looping Back:", len(fail))
					fail = []SendFile{}
				default:
					break
				}
			}
			for i, dc := range ch.Done { // Send out any done.
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
			case files, ok := <-ch.In: // Get incoming.
				if !ok {
					// Shouldn't ever get here since incoming channel should be open
					// until loop channel closed, which only happens [above] just before
					// returning.  But leaving it here for completeness' sake.
					ch.In = nil
					break
				}
				for _, file := range files {
					if file.GetCancel() {
						df := &pollFile{file: file}
						for i := range done {
							done[i] = append(done[i], df)
						}
						continue
					}
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
		var f []PollFile
		var d []PollFile
		f, poll = poller.buildList()
		for _, ff := range f {
			fail = append(fail, ff.GetOrigFile().(SendFile))
		}
		if len(poll) < 1 {
			continue
		}
		_, f, d, err = poller.Poll(poll)
		if err != nil {
			logging.Error(err.Error())
		} else {
			// Only increase the number of poll attempts if the poll request did not result in error.
			for _, pf := range poll {
				poller.Files[pf.GetRelPath()].nPoll++
			}
		}
		if loop > 0 {
			loop -= len(d) // Offset the loop counter so we know when the "retry" loop is empty.
		}
		for _, ff := range f {
			fail = append(fail, ff.GetOrigFile().(SendFile))
			poller.removeFile(ff.(PollFile))
		}
		var df []DoneFile
		for _, dd := range d {
			df = append(df, dd.(DoneFile))
			poller.removeFile(dd.(PollFile))
		}
		for i := range done {
			done[i] = append(done[i], df...)
		}
		t = time.Now()
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

func (poller *Poller) buildList() (fail []PollFile, poll []PollFile) {
	for _, pf := range poller.Files {
		if poller.conf.PollAttempts > 0 && pf.nPoll > poller.conf.PollAttempts {
			logging.Debug("POLL Rejected:", pf.file.GetRelPath())
			fail = append(fail, pf)
			poller.removeFile(pf)
			continue
		}
		if time.Now().Sub(pf.time) > poller.conf.PollDelay {
			logging.Debug("POLL Request:", pf.file.GetRelPath())
			poll = append(poll, pf)
		}
	}
	return
}

// Poll attempts to confirm whether files have been properly received on target.
func (poller *Poller) Poll(files []PollFile) (none []PollFile, fail []PollFile, pass []PollFile, err error) {
	fmap := make(map[string]PollFile)
	var cf []*ConfirmFile
	for _, f := range files {
		cf = append(cf, &ConfirmFile{f.GetRelPath(), f.GetStarted().Unix()})
		fmap[f.GetRelPath()] = f
		logging.Debug("POLLing:", f.GetRelPath())
	}
	payloadJSON, err := json.Marshal(cf)
	if err != nil {
		return
	}
	url := fmt.Sprintf("%s://%s/validate", poller.conf.Protocol(), poller.conf.TargetHost)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payloadJSON))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add(HeaderSourceName, poller.conf.SourceName)
	if poller.conf.TargetKey != "" {
		req.Header.Add(HeaderKey, poller.conf.TargetKey)
	}
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
		logging.Debug("POLL Result:", path, code)
		pf := fmap[path]
		switch code {
		case ConfirmNone:
			none = append(none, pf)
		case ConfirmPassed:
			pass = append(pass, pf)
		case ConfirmFailed:
			fail = append(fail, pf)
		}
	}
	return
}
