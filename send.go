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
	cancel     bool
	lock       sync.RWMutex
}

func newSendFile(f SortFile) (file *sendFile, err error) {
	file = &sendFile{}
	file.file = f
	if _, ok := f.GetOrigFile().(RecoverFile); ok {
		file.hash = f.GetOrigFile().(RecoverFile).GetHash()
	} else {
		file.hash, err = fileutils.FileMD5(f.GetPath(true))
	}
	return
}

func (f *sendFile) GetPath(follow bool) string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.file.GetPath(follow)
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
	prev := f.file.GetPrevReq()
	if prev != nil {
		return prev.GetRelPath()
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

func (f *sendFile) Stat() (bool, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.file.GetOrigFile().Reset()
}

func (f *sendFile) Reset() (changed bool, err error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.started = 0
	f.bytesAlloc = 0
	f.bytesSent = 0
	if _, ok := f.file.GetOrigFile().(RecoverFile); ok {
		changed, err = f.file.GetOrigFile().Reset()
		return
	}
	changed, err = f.file.GetOrigFile().Reset()
	if err != nil {
		return
	}
	if changed {
		f.hash, err = fileutils.FileMD5(f.GetPath(true))
		if err != nil {
			return
		}
	}
	return
}

func (f *sendFile) GetCancel() bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.cancel
}

func (f *sendFile) SetCancel(c bool) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.cancel = c
}

// SenderConf contains all parameters necessary for the sending daemon.
type SenderConf struct {
	Threads      int
	Compress     bool
	SourceName   string
	TargetName   string
	TargetHost   string
	TLSCert      string
	TLSKey       string
	BinSize      units.Base2Bytes
	Timeout      time.Duration
	PollInterval time.Duration
	PollDelay    time.Duration
	PollAttempts int
}

// Protocol returns the protocl (http vs https) based on the configuration.
func (sc *SenderConf) Protocol() string {
	if sc.TLSCert != "" && sc.TLSKey != "" {
		return "https"
	}
	return "http"
}

// SenderChan is the struct used to house the channels in support of flow
// through the sender.
type SenderChan struct {
	In       <-chan SortFile
	Retry    <-chan []SendFile
	Done     []chan<- []SendFile
	Close    chan<- bool
	sendFile chan SendFile
	sendBin  chan *Bin
}

// Sender is the struct for managing STS send operations.
type Sender struct {
	conf   *SenderConf
	ch     *SenderChan
	client *http.Client
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

// Start controls the sending of files on the in channel and writing results to the out channels.
func (sender *Sender) Start(ch *SenderChan) {
	sender.ch = ch
	sender.ch.sendFile = make(chan SendFile, sender.conf.Threads)
	sender.ch.sendBin = make(chan *Bin, sender.conf.Threads*2)
	var wgWrap, wgBin, wgRebin, wgSend sync.WaitGroup
	nWrap, nBin, nRebin, nSend := 1, 1, 1, sender.conf.Threads
	wgWrap.Add(nWrap)
	for i := 0; i < nWrap; i++ {
		go sender.startWrap(&wgWrap)
	}
	wgBin.Add(nBin)
	for i := 0; i < nBin; i++ {
		go sender.startBin(&wgBin)
	}
	wgRebin.Add(nRebin)
	for i := 0; i < nRebin; i++ {
		go sender.startRebin(&wgRebin)
	}
	wgSend.Add(nSend)
	for i := 0; i < nSend; i++ {
		go sender.startSend(&wgSend)
	}
	wgWrap.Wait()
	logging.Debug("SEND Wrapping Done")
	for i := 0; i < nBin; i++ {
		sender.ch.sendFile <- nil // Trigger a shutdown without closing the channel.
	}
	wgBin.Wait()
	logging.Debug("SEND Binning Done")
	for i := 0; i < nSend; i++ {
		sender.ch.sendBin <- nil // Trigger a shutdown without closing the channel.
	}
	wgSend.Wait()
	logging.Debug("SEND Sending Done")

	// Once we're here, everything has been sent to the out channel(s) and all we need to
	// do is resend anything that comes in on the retry channel.
	sender.ch.Close <- true // Communicate shutdown.
	close(sender.ch.Close)
	wgBin.Add(nBin)
	for i := 0; i < nBin; i++ {
		go sender.startBin(&wgBin) // Restart the binners.
	}
	wgSend.Add(nSend)
	for i := 0; i < nSend; i++ {
		go sender.startSend(&wgSend) // Restart the senders.
	}
	wgRebin.Wait() // Will only unblock once retry channel is closed. Then we can close out the loop.
	logging.Debug("SEND Retrying Done")
	close(sender.ch.sendFile)
	wgBin.Wait()
	close(sender.ch.sendBin)
	wgSend.Wait()
	for _, c := range sender.ch.Done {
		close(c)
	}
}

func (sender *Sender) startWrap(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		f, ok := <-sender.ch.In
		if !ok {
			break
		}
		logging.Debug("SEND Found:", f.GetRelPath())
		sf, err := newSendFile(f)
		if err != nil {
			logging.Error("Failed to get file ready to send:", f.GetPath(false), err.Error())
			return
		}
		sender.ch.sendFile <- sf
	}
}

func (sender *Sender) startBin(wg *sync.WaitGroup) {
	defer wg.Done()
	var b *Bin
	var f SendFile
	var ok bool
	wait := time.Millisecond * 100
	waited := time.Millisecond * 0
	in := sender.ch.sendFile
	for {
		if f == nil {
			select {
			case f, ok = <-in:
				if !ok || f == nil {
					in = nil
					continue
				}
				logging.Debug("SEND Binning Found:", f.GetRelPath())
				waited = time.Millisecond * 0
				if f.IsSent() {
					for _, c := range sender.ch.Done {
						c <- []SendFile{f}
					}
					f = nil
					continue
				}
			default:
				time.Sleep(wait)
				waited += wait
				if (in == nil || waited > time.Second) && b != nil && len(b.Parts) > 0 {
					// Time to send the bin even if it's not full.
					sender.ch.sendBin <- b
					b = nil
				}
				if in == nil {
					logging.Debug("SEND Exit Binning", b)
					return
				}
				continue
			}
		}
		if b == nil {
			b = NewBin(int64(sender.conf.BinSize)) // Start new bin.
		}
		added, err := b.Add(f)
		if err != nil {
			logging.Error("Failed to generate partial:", f.GetRelPath(), err.Error())
			f = nil
			continue
		}
		if !added || f.GetBytesAlloc() == f.GetSize() { // File is fully allocated.
			f = nil
		}
		if b.IsFull() {
			sender.ch.sendBin <- b
			b = nil
		}
	}
}

func (sender *Sender) startRebin(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		f, ok := <-sender.ch.Retry
		if !ok {
			break
		}
		var cancel []SendFile
		for _, sf := range f {
			logging.Debug("SEND Resend:", sf.GetRelPath())
			// If we need to resend a file, let's make sure it hasn't changed...
			changed, err := sf.Reset()
			if err != nil {
				logging.Error("Failed to reset:", sf.GetPath(false), err.Error())
				sf.SetCancel(true)
				cancel = append(cancel, sf)
				continue
			}
			if changed {
				logging.Debug("SEND File Changed:", sf.GetPath(false))
			}
			sender.ch.sendFile <- sf
		}
		sender.done(cancel)
	}
}

func (sender *Sender) startSend(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		b, ok := <-sender.ch.sendBin
		if !ok || b == nil {
			break
		}
		for _, p := range b.Parts {
			logging.Debug("SEND Bin Part:", &b, p.File.GetRelPath(), p.Beg, p.End)
		}
		sender.sendBin(b)
	}
}

// sendBin sends a bin in a loop to handle errors and keep trying.
func (sender *Sender) sendBin(bin *Bin) {
	// TODO: respond to partially successful requests.
	nerr := 0
	for {
		err := sender.httpBin(bin)
		if err == nil {
			break
		}
		nerr++
		logging.Error(err.Error())
		canceled := bin.Validate()
		if len(canceled) > 0 {
			sender.done(canceled)
			if len(bin.Parts) > 0 {
				continue
			}
			break
		}
		time.Sleep(time.Duration(nerr) * time.Second) // Wait longer the more it fails.
	}
	var done []SendFile
	for _, part := range bin.Parts {
		logging.Debug("SEND Add Sent:", part.File.GetRelPath(), part.End-part.Beg)
		part.File.AddSent(part.End - part.Beg)
		if part.File.IsSent() {
			f := part.File
			logging.Debug("SEND Sent:", f.GetRelPath(), f.GetSize(), f.TimeMs())
			logging.Sent(f.GetRelPath(), f.GetHash(), f.GetSize(), f.TimeMs(), sender.conf.TargetName)
			done = append(done, f)
		}
	}
	sender.done(done)
}

func (sender *Sender) done(f []SendFile) {
	if len(f) == 0 {
		return
	}
	for _, c := range sender.ch.Done {
		c <- f
	}
}

func (sender *Sender) httpBin(bin *Bin) (err error) {
	var bw BinEncoder
	if sender.conf.Compress {
		bw = NewZBinWriter(NewBinWriter(bin))
	}
	if !sender.conf.Compress || err != nil {
		bw = NewBinWriter(bin)
	}
	meta, err := bw.EncodeMeta()
	if err != nil {
		return
	}
	url := fmt.Sprintf("%s://%s/data", sender.conf.Protocol(), sender.conf.TargetHost)
	req, err := http.NewRequest("PUT", url, bw)
	if err != nil {
		return
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
		return
	}
	if resp.StatusCode == 206 {
		err = fmt.Errorf("Bin failed validation")
		return
	} else if resp.StatusCode != 200 {
		err = fmt.Errorf("Bin failed with response code: %d", resp.StatusCode)
		return
	}
	resp.Body.Close()
	req.Close = true
	return
}
