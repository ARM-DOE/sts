package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
	"github.com/alecthomas/units"
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

func newSendFile(f SortFile) (file *sendFile, err error) {
	file = &sendFile{}
	file.file = f
	if _, ok := f.GetOrigFile().(RecoverFile); ok {
		file.hash = f.GetOrigFile().(RecoverFile).GetHash()
	} else {
		file.hash, err = fileutils.FileMD5(f.GetPath())
	}
	return
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
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		p := f.file.GetOrigFile().(RecoverFile).GetPrevName()
		if p != "" {
			return p
		}
	}
	if f.file.GetPrev() != nil {
		return f.file.GetPrev().GetRelPath()
	}
	return ""
}

func (f *sendFile) GetStarted() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.started
}

func (f *sendFile) GetNextAlloc() (int64, int64) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		return f.file.GetOrigFile().(RecoverFile).GetNextAlloc()
	}
	return f.bytesAlloc, f.file.GetSize()
}

func (f *sendFile) GetBytesAlloc() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		return f.file.GetOrigFile().(RecoverFile).GetBytesAlloc()
	}
	return f.bytesAlloc
}

func (f *sendFile) AddAlloc(bytes int64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.started == 0 {
		f.started = time.Now().UnixNano()
	}
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		f.file.GetOrigFile().(RecoverFile).AddAlloc(bytes)
		return
	}
	f.bytesAlloc += bytes
}

func (f *sendFile) GetBytesSent() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		return f.file.GetOrigFile().(RecoverFile).GetBytesSent()
	}
	return f.bytesSent
}

func (f *sendFile) AddSent(bytes int64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		f.file.GetOrigFile().(RecoverFile).AddSent(bytes)
		return
	}
	f.bytesSent += bytes
}

func (f *sendFile) TimeMs() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return int64((time.Now().UnixNano() - f.started) / 1e6)
}

func (f *sendFile) IsSent() bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		return f.file.GetOrigFile().(RecoverFile).GetBytesSent() == f.file.GetSize()
	}
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
	BinSize      units.Base2Bytes
	Timeout      time.Duration
	MaxRetries   int
	PollInterval time.Duration
	PollDelay    time.Duration
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
func (sender *Sender) StartSend(fileChan chan SendFile, doneChan chan []SendFile) {
	var bin *Bin
	var file SendFile
	var done []SendFile
	// Outer loop for building and sending bins.
	for {
		// Send any completed files to the done channel.
		if len(done) > 0 {
			select {
			case doneChan <- done:
				done = []SendFile{} // Clear them out once passed on.
			default:
				break
			}
		}
		if file != nil {
			// Attempt to put this file back on the file channel so others can help.
			// If not, just keep working on it and try again later if we're still not done on next iteration.
			select {
			case fileChan <- file:
				file = nil
			default:
				break
			}
		}
		bin = NewBin(int64(sender.conf.BinSize)) // Start new bin.
		// Loop until we have a bin ready to send.
		for {
			waited := time.Millisecond * 0
			// While we don't have a file from the channel or we haven't waited long enough.
			for file == nil {
				select {
				case file = <-fileChan:
					waited = 0
				default:
					if waited.Seconds() > 1 && len(bin.Parts) > 0 {
						break
					}
					wait := time.Millisecond * 100
					time.Sleep(wait)
					waited += wait
					continue
				}
				break
			}
			// No file found, we must have waited long enough and can go ahead and send this bin.
			if file == nil {
				break
			}
			// This only happens when a recovered file was already fully sent and just needs to move on.
			// It goes the whole path in order to be sorted and wrapped like the others.
			if file.IsSent() {
				logging.Debug("SEND Found (Sent):", file.GetRelPath())
				done = append(done, file)
				file = nil
				continue
			}
			// File found; let's allocate what we can to this bin.
			logging.Debug("SEND Found:", file.GetRelPath(), bin.IsFull(), bin.BytesLeft, file.GetSize()-file.GetBytesAlloc())
			if bin.Add(file) && file.GetBytesAlloc() < file.GetSize() { // File is not fully allocated.
				logging.Debug("SEND Loop:", file.GetRelPath(), file.GetBytesAlloc())
			} else {
				// Reset so next file can be grabbed.
				file = nil
			}
			if bin.IsFull() { // Bin is full. Time to send it.
				break
			}
		}
		done = append(done, sender.out(bin)...)
	}
}

func (sender *Sender) out(bin *Bin) []SendFile {
	// Send the bin.  In a loop to handle errors and keep trying.
	// TODO: respond to partially successful requests.
	var done []SendFile
	for {
		logging.Debug("SEND Bin:", bin.Bytes, bin.BytesLeft, len(bin.Parts))
		err := sender.send(bin)
		if err != nil {
			logging.Error(err.Error())
			sender.nFailures++
			time.Sleep(time.Duration(sender.nFailures) * time.Second) // Wait longer the more it fails.
			continue
		}
		for _, part := range bin.Parts {
			part.File.AddSent(part.End - part.Beg)
			if part.File.IsSent() {
				f := part.File
				logging.Sent(f.GetRelPath(), f.GetHash(), f.GetSize(), f.TimeMs(), sender.conf.TargetName)
				done = append(done, f)
			}
		}
		bin = nil
		break
	}
	return done
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
	if resp.StatusCode == 206 {
		return fmt.Errorf("Bin failed validation")
	} else if resp.StatusCode != 200 {
		return fmt.Errorf("Bin failed with response code: %d", resp.StatusCode)
	}
	resp.Body.Close()
	req.Close = true
	return nil
}
