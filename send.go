package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
)

type sendFile struct {
	file       SortFile
	hash       string
	prevName   string
	started    int64
	bytesAlloc int64
	bytesSent  int64
	lock       sync.RWMutex
}

func newSendFile(f SortFile) (*sendFile, error) {
	file := &sendFile{}
	file.file = f
	var err error
	file.hash, err = fileutils.FileMD5(f.GetPath())
	return file, err
}

func (f *sendFile) GetSortFile() SortFile {
	return f.file
}

func (f *sendFile) GetPath() string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.file.GetPath()
}

func (f *sendFile) GetRelPath() string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.file.GetRelPath()
}

func (f *sendFile) GetSize() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.file.GetSize()
}

func (f *sendFile) GetTime() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.file.GetTime()
}

func (f *sendFile) GetHash() string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.hash
}

func (f *sendFile) GetPrevName() string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if f.file.GetPrevByTag() != nil {
		return f.file.GetPrevByTag().GetRelPath()
	}
	return ""
}

func (f *sendFile) GetStarted() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.started
}

func (f *sendFile) GetBytesAlloc() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.bytesAlloc
}

func (f *sendFile) GetBytesSent() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.bytesSent
}

func (f *sendFile) AddAlloc(bytes int64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.bytesAlloc += bytes
}

func (f *sendFile) AddSent(bytes int64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.bytesSent == 0 {
		f.started = time.Now().UnixNano()
	}
	f.bytesSent += bytes
}

func (f *sendFile) Time() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return time.Now().UnixNano() - f.started
}

func (f *sendFile) IsSent() bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.file.GetSize() == f.bytesSent
}

// SenderConf contains all parameters necessary for the sending daemon.
type SenderConf struct {
	Compress     bool
	SourceName   string
	TargetName   string
	TargetHost   string
	TLSCert      string
	TLSKey       string
	BinSize      int64
	Timeout      int
	MaxRetries   int
	PollInterval int
	PollDelay    int
}

// Protocol returns the protocl (http vs https) based on the configuration.
func (sc *SenderConf) Protocol() string {
	if sc.TLSCert != "" && sc.TLSKey != "" {
		return "https"
	}
	return "http"
}

// Sender is the struct for managing STS send operations.
type Sender struct {
	conf      *SenderConf
	client    *http.Client
	nFailures int // Number of sequential failures used to gauge network thrashing
}

// NewSender creates a new sender instance based on provided configuration.
func NewSender(conf *SenderConf) *Sender {
	sender := &Sender{}
	sender.conf = conf

	var err error
	sender.client, err = httputils.GetClient(conf.TLSCert, conf.TLSKey)
	if err != nil {
		logging.Error(err.Error())
	}

	sender.client.Timeout = time.Second * time.Duration(conf.Timeout)
	return sender
}

// StartPrep is the daemon that converts a SortFile to a SendFile (includes MD5 computation).
func (sender *Sender) StartPrep(prepChan chan SortFile, fileChan chan SendFile) {
	for {
		file := <-prepChan
		logging.Debug("PRESEND Found:", file.GetRelPath())
		sendFile, err := newSendFile(file)
		if err != nil {
			logging.Error("Failed to get file ready to send:", file.GetPath(), err.Error())
			continue
		}
		fileChan <- sendFile
	}
}

// StartSend is the daemon for sending files on the incoming channel and routing them to the
// "done" channel after all parts have been sent.  The "loop" channel is for files that aren't
// yet fully allocated.
func (sender *Sender) StartSend(fileChan chan SendFile, loopChan chan SendFile, doneChan []chan SendFile) {
	var bin *Bin
	var file SendFile
	for {
		bin = NewBin(sender.conf.BinSize)
		waited := time.Millisecond * 0
		isLast := false
		for {
			file = nil
			isLast = false
			select {
			case file = <-loopChan:
				break
			default:
				select {
				case file = <-fileChan:
					waited = 0
					break
				default:
					if waited.Seconds() > 1 && len(bin.Parts) > 0 {
						isLast = true
					}
					wait := time.Millisecond * 100
					time.Sleep(wait)
					waited += wait
				}
			}
			if file == nil {
				if isLast {
					break
				}
				continue
			}
			logging.Debug("SEND Found:", file.GetRelPath(), bin.IsFull(), bin.BytesLeft)
			bin.Add(file)
			if file.GetBytesAlloc() < file.GetSize() {
				logging.Debug("SEND Loop:", file.GetRelPath(), file.GetBytesAlloc())
				loopChan <- file
				break
			}
			if bin.IsFull() {
				break
			}
		}
		if len(bin.Parts) == 0 {
			continue
		}
		for {
			logging.Debug("SEND Bin:", bin.Bytes, bin.BytesLeft, len(bin.Parts))
			err := sender.send(bin)
			if err != nil {
				sender.nFailures++
				time.Sleep(time.Duration(sender.nFailures) * time.Second) // Wait longer the more it fails.
				continue
			}
			sender.complete(bin, doneChan)
			bin = nil
			break
		}
	}
}

func (sender *Sender) send(bin *Bin) error {
	var err error
	for i := range bin.Parts {
		_, err = bin.Parts[i].GetHash()
		if err != nil {
			return fmt.Errorf("Failed to compute bin part hash: %s", err.Error())
		}
	}
	var bw BinEncoder
	if sender.conf.Compress {
		bw = NewZBinWriter(NewBinWriter(bin))
	}
	if !sender.conf.Compress || err != nil {
		bw = NewBinWriter(bin)
	}
	meta, err := bw.EncodeMeta()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s://%s/data", sender.conf.Protocol(), sender.conf.TargetHost)
	req, err := http.NewRequest("PUT", url, bw)
	if err != nil {
		return err
	}
	req.Header.Add(HeaderSourceName, sender.conf.SourceName)
	req.Header.Add(HeaderBinData, meta)
	req.Header.Add("Transfer-Encoding", "chunked")
	req.Header.Set("Connection", "close") // Prevents persistent connections opening too many file handles
	req.ContentLength = -1
	_, ok := bw.(*ZBinWriter)
	if ok {
		req.Header.Add("Content-Encoding", "gzip")
	}
	resp, err := sender.client.Do(req)
	if err != nil {
		return err
	}
	code, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	} else if string(code) == "206" {
		return fmt.Errorf("Bin failed validation: %s", bin.Path)
	} else if string(code) != "200" {
		return fmt.Errorf("Send failed with response code: %s", string(code))
	}
	resp.Body.Close()
	req.Close = true
	return nil
}

func (sender *Sender) complete(bin *Bin, done []chan SendFile) {
	for _, part := range bin.Parts {
		part.File.AddSent(part.End - part.Start)
		if part.File.IsSent() {
			f := part.File
			logging.Sent(f.GetRelPath(), f.GetHash(), f.GetSize(), f.GetTime(), sender.conf.TargetName)
			for _, dc := range done {
				dc <- part.File // This will block if channel is full so it's imoprtant that the receiving end of this channel is able to keep up.
			}
		}
	}
}
