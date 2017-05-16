package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

// ConfirmWaiting is the indicator that file confirmation succeeded but its
// predecessor has not been confirmed.
const ConfirmWaiting = 3

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
	p.client, err = httputils.GetClient(conf.TLS)
	if err != nil {
		logging.Error(err.Error())
	}
	return p
}

// Start runs the poller and goes until we get input on the stop channel.
func (poller *Poller) Start(ch *PollerChan) {
	var t time.Time
	var err error
	var poll []PollFile
	fail := []SendFile{}
	done := make([][]DoneFile, len(ch.Done))
	pause := time.Millisecond * 100
	for {
		select {
		// We needed a way to communicate shutdown without closing the main input channel because the poller
		// operates on a loop with failed files.
		case s, ok := <-ch.Stop:
			if s || !ok {
				logging.Debug("POLL Got Shutdown Signal")
				close(ch.Fail)
				ch.Stop = nil // Select will always drop to default (starting next iteration).
				ch.Fail = nil
			}
		default:
			// If we got the shutdown signal that means everything is in our court.
			// So if we have nothing in the queue, then we're done.
			if ch.Stop == nil && len(poller.Files) == 0 {
				empty := true
				for i, dc := range ch.Done {
					if len(done[i]) > 0 {
						empty = false
						continue
					}
					close(dc)
				}
				if empty {
					return
				}
			}
			break
		}
		time.Sleep(pause) // To keep from thrashing
		for {
			if len(fail) > 0 && ch.Fail != nil { // Send out any failed.
				logging.Debug("POLL Failed:", len(fail))
				select {
				case ch.Fail <- fail:
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
		var n []PollFile
		var f []PollFile
		var d []PollFile
		f, poll = poller.buildList()
		for _, ff := range f {
			fail = append(fail, ff.GetOrigFile().(SendFile))
			// Cancel files that have exceeded maximum poll attempts.
			ff.GetOrigFile().(SendFile).SetCancel(true)
		}
		if len(poll) < 1 {
			continue
		}
		nerr := 0
		for {
			n, f, d, _, err = poller.Poll(poll)
			if err == nil {
				break
			}
			nerr++
			logging.Error("Poll failed:", err.Error())
			time.Sleep(time.Duration(nerr) * time.Second) // Wait longer the more it fails.
		}
		// Only increase the poll count for "none" files.
		// Files that are "waiting" should continue to be polled since eventually
		// (even if the previous file is never found) the file will be released.
		for _, pf := range n {
			poller.Files[pf.GetRelPath()].nPoll++
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
func (poller *Poller) Poll(files []PollFile) (none []PollFile, fail []PollFile, pass []PollFile, wait []PollFile, err error) {
	fmap := make(map[string]PollFile)
	var cf []*ConfirmFile
	for _, f := range files {
		cf = append(cf, &ConfirmFile{f.GetRelPath(), f.GetStarted().Unix()})
		fmap[f.GetRelPath()] = f
		logging.Debug("POLLing:", f.GetRelPath())
	}
	url := fmt.Sprintf("%s://%s/validate", poller.conf.Protocol(), poller.conf.TargetHost)
	r, err := httputils.GetJSONReader(cf, poller.conf.Compression)
	if err != nil {
		return
	}
	req, err := http.NewRequest("POST", url, r)
	if poller.conf.Compression != gzip.NoCompression {
		req.Header.Add(httputils.HeaderContentEncoding, httputils.HeaderGzip)
	}
	req.Header.Add(httputils.HeaderContentType, httputils.HeaderJSON)
	req.Header.Add(httputils.HeaderSourceName, poller.conf.SourceName)
	req.Header.Add(httputils.HeaderSep, string(os.PathSeparator))
	if poller.conf.TargetKey != "" {
		req.Header.Add(httputils.HeaderKey, poller.conf.TargetKey)
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
	sep := resp.Header.Get(httputils.HeaderSep)
	reader, err := httputils.GetRespReader(resp)
	if err != nil {
		return
	}
	defer reader.Close()
	fileMap := make(map[string]int)
	jsonDecoder := json.NewDecoder(reader)
	err = jsonDecoder.Decode(&fileMap)
	if err != nil {
		return
	}
	for path, code := range fileMap {
		if sep != "" {
			path = filepath.Join(strings.Split(path, sep)...)
		}
		logging.Debug("POLL Result:", path, code)
		pf := fmap[path]
		switch code {
		case ConfirmNone:
			none = append(none, pf)
		case ConfirmPassed:
			pass = append(pass, pf)
		case ConfirmFailed:
			fail = append(fail, pf)
		case ConfirmWaiting:
			wait = append(wait, pf)
		}
	}
	return
}
