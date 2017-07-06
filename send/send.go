package send

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/bin"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/httputil"
	"code.arm.gov/dataflow/sts/logging"
	"github.com/alecthomas/units"
)

type sendFile struct {
	file       sts.SortFile
	hash       string
	prevName   string
	started    time.Time
	completed  time.Time
	bytesAlloc int64
	bytesSent  int64
	cancel     bool
	lock       sync.RWMutex
}

func newSendFile(f sts.SortFile) (file *sendFile, err error) {
	file = &sendFile{}
	file.file = f
	if _, ok := f.GetOrigFile().(sts.RecoverFile); ok {
		file.hash = f.GetOrigFile().(sts.RecoverFile).GetHash()
	} else {
		file.hash, err = fileutil.FileMD5(f.GetPath(true))
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
	prev := f.file.GetPrevReq()
	if prev != nil {
		return prev.GetRelPath()
	}
	return ""
}

func (f *sendFile) SetStarted(t time.Time) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.started.IsZero() || t.Before(f.started) {
		f.started = t
	}
}

func (f *sendFile) GetStarted() time.Time {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.started
}

func (f *sendFile) SetCompleted(t time.Time) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if t.After(f.completed) {
		f.completed = t
	}
}

func (f *sendFile) GetCompleted() time.Time {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.completed
}

func (f *sendFile) GetNextAlloc() (int64, int64) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		return f.file.GetOrigFile().(sts.RecoverFile).GetNextAlloc()
	}
	return f.bytesAlloc, f.file.GetSize()
}

func (f *sendFile) GetBytesAlloc() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		return f.file.GetOrigFile().(sts.RecoverFile).GetBytesAlloc()
	}
	return f.bytesAlloc
}

func (f *sendFile) AddAlloc(bytes int64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		f.file.GetOrigFile().(sts.RecoverFile).AddAlloc(bytes)
		return
	}
	f.bytesAlloc += bytes
}

func (f *sendFile) GetBytesSent() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		return f.file.GetOrigFile().(sts.RecoverFile).GetBytesSent()
	}
	return f.bytesSent
}

func (f *sendFile) AddSent(bytes int64) bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		f.file.GetOrigFile().(sts.RecoverFile).AddSent(bytes)
	} else {
		f.bytesSent += bytes
	}
	return f.isSent()
}

func (f *sendFile) TimeMs() int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	if !f.completed.IsZero() && !f.started.IsZero() {
		return int64(f.completed.Sub(f.started).Nanoseconds() / 1e6)
	}
	return -1
}

func (f *sendFile) IsSent() bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.isSent()
}

func (f *sendFile) isSent() bool {
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		return f.file.GetOrigFile().(sts.RecoverFile).GetBytesSent() == f.file.GetSize()
	}
	return f.file.GetSize() == f.bytesSent
}

func (f *sendFile) Stat() (bool, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		return f.file.GetOrigFile().(sts.RecoverFile).Stat()
	}
	return f.file.GetOrigFile().Reset()
}

func (f *sendFile) Reset() (changed bool, err error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.started = time.Time{}
	if _, ok := f.file.GetOrigFile().(sts.RecoverFile); ok {
		changed, err = f.file.GetOrigFile().Reset()
		// Return because the RecoverFile recomputes MD5 itself.
		return
	}
	f.bytesAlloc = 0
	f.bytesSent = 0
	changed, err = f.file.GetOrigFile().Reset()
	if err != nil {
		return
	}
	if changed {
		f.hash, err = fileutil.FileMD5(f.GetPath(true))
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
	Compression  int
	SourceName   string
	TargetName   string
	TargetHost   string
	TargetKey    string
	TLS          *tls.Config
	BinSize      units.Base2Bytes
	Timeout      time.Duration
	StatInterval time.Duration
	PollInterval time.Duration
	PollDelay    time.Duration
	PollAttempts int
}

// Protocol returns the protocl (http vs https) based on the configuration.
func (sc *SenderConf) Protocol() string {
	if sc.TLS != nil {
		return "https"
	}
	return "http"
}

// SenderChan is the struct used to house the channels in support of flow
// through the sender.
type SenderChan struct {
	In       <-chan sts.SortFile
	Retry    <-chan []sts.SendFile
	Done     []chan<- []sts.SendFile
	Close    chan<- bool
	sendFile chan sts.SendFile
	sendBin  chan *bin.Bin
	doneBin  chan *bin.Bin
}

// Sender is the struct for managing STS send operations.
type Sender struct {
	conf      *SenderConf
	ch        *SenderChan
	client    *http.Client
	statSince time.Time
	statLock  sync.RWMutex
	bytesOut  int64
}

// NewSender creates a new sender instance based on provided configuration.
func NewSender(conf *SenderConf) (s *Sender, err error) {
	s = &Sender{}
	s.conf = conf
	if s.client, err = httputil.GetClient(conf.TLS); err != nil {
		return
	}
	s.client.Timeout = time.Second * time.Duration(conf.Timeout)
	s.statSince = time.Now()
	return
}

// Start controls the sending of files on the in channel and writing results to the out channels.
func (sender *Sender) Start(ch *SenderChan) {
	sender.ch = ch
	sender.ch.sendFile = make(chan sts.SendFile, sender.conf.Threads)
	sender.ch.sendBin = make(chan *bin.Bin, sender.conf.Threads*2)
	sender.ch.doneBin = make(chan *bin.Bin, sender.conf.Threads)
	var wgWrap, wgBin, wgRebin, wgSend, wgDone sync.WaitGroup
	nWrap, nBin, nRebin, nSend, nDone := 1, 1, 1, sender.conf.Threads, 1
	start := func(s func(wg *sync.WaitGroup), wg *sync.WaitGroup, n int) {
		wg.Add(n)
		for i := 0; i < n; i++ {
			go s(wg)
		}
	}

	// Start the go routines.
	start(sender.startWrap, &wgWrap, nWrap)
	start(sender.startBin, &wgBin, nBin)
	start(sender.startRebin, &wgRebin, nRebin)
	start(sender.startSend, &wgSend, nSend)
	start(sender.startDone, &wgDone, nDone)

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
	for i := 0; i < nDone; i++ {
		sender.ch.doneBin <- nil // Trigger a shutdown without closing the channel.
	}
	wgDone.Wait()
	logging.Debug("SEND Output Done")

	// Once we're here, everything has been sent to the out channel(s) and all we need to
	// do is resend anything that comes in on the retry channel.
	sender.ch.Close <- true // Communicate shutdown.
	close(sender.ch.Close)

	// Restart to finish any retries.
	start(sender.startBin, &wgBin, nBin)
	start(sender.startSend, &wgSend, nSend)
	start(sender.startDone, &wgDone, nDone)

	wgRebin.Wait() // Will only unblock once retry channel is closed. Then we can close out the loop.
	logging.Debug("SEND Retrying Done")
	close(sender.ch.sendFile)
	wgBin.Wait()
	close(sender.ch.sendBin)
	wgSend.Wait()
	close(sender.ch.doneBin)
	wgDone.Wait()
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
		if sf.IsSent() {
			logging.Debug("SEND File Already Done:", f.GetRelPath())
			// If we get here then this is a "RecoverFile" that was fully sent
			// before a crash and we need to make sure it got logged as "sent"
			// and if not, we need to do it, even though we won't know how long
			// the original transfer took.
			if !logging.FindSent(sf.GetRelPath(), time.Unix(sf.GetTime(), 0), time.Now(), sender.conf.TargetName) {
				logging.Sent(sf.GetRelPath(), sf.GetHash(), sf.GetSize(), 0, sender.conf.TargetName)
			}
			for _, c := range sender.ch.Done {
				c <- []sts.SendFile{sf}
			}
			continue
		}
		sender.ch.sendFile <- sf
	}
}

func (sender *Sender) startBin(wg *sync.WaitGroup) {
	defer wg.Done()
	var b *bin.Bin
	var f sts.SendFile
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
			b = bin.NewBin(int64(sender.conf.BinSize)) // Start new bin.
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
		var cancel []sts.SendFile
		for _, sf := range f {
			logging.Debug("SEND Resend:", sf.GetRelPath())
			// The poller will cancel a file if it exceeded its maximum allowed
			// polling attempts.
			if sf.GetCancel() {
				cancel = append(cancel, sf)
				continue
			}
			// If we need to resend a file, let's make sure it hasn't changed.
			// Since bin validation cancels changed files we should do the same.
			changed, err := sf.Reset()
			if changed || err != nil {
				if changed {
					logging.Debug("SEND File Changed:", sf.GetPath(false))
				}
				if err != nil {
					logging.Error("Failed to reset:", sf.GetPath(false), err.Error())
				}
				sf.SetCancel(true)
				cancel = append(cancel, sf)
				continue
			}
			// We only get here if the file failed validation on the receiving
			// end and the file hasn't changed locally.
			sender.ch.sendFile <- sf
		}
		sender.done(cancel)
	}
}

func (sender *Sender) startSend(wg *sync.WaitGroup) {
	defer wg.Done()
	var gz *gzip.Writer
	if sender.conf.Compression != gzip.NoCompression {
		var err error
		gz, err = gzip.NewWriterLevel(nil, sender.conf.Compression)
		if err != nil {
			logging.Error(err.Error())
			return
		}
	}
	for {
		b, ok := <-sender.ch.sendBin
		if !ok || b == nil {
			break
		}
		for _, p := range b.Parts {
			logging.Debug("SEND Bin Part:", &b, p.File.GetRelPath(), p.Beg, p.End)
		}
		sender.sendBin(b, gz)
	}
}

func (sender *Sender) startDone(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		b, ok := <-sender.ch.doneBin
		if !ok || b == nil {
			break
		}
		var done []sts.SendFile
		for _, p := range b.Parts {
			f := p.File
			f.SetStarted(b.GetStarted())
			if f.AddSent(p.End - p.Beg) {
				f.SetCompleted(b.GetCompleted())
				ms := f.TimeMs()
				if p.Beg == 0 && p.End == f.GetSize() {
					// If the file fit entirely in a bin, we can compute a more
					// precise send time.
					n := b.GetCompleted().Sub(b.GetStarted()).Nanoseconds()
					r := float64(0)
					if b.Bytes > b.BytesLeft {
						r = float64(p.End-p.Beg) / float64(b.Bytes-b.BytesLeft)
					}
					d := time.Duration(float64(n) * r)
					ms = int64((r * float64(d.Nanoseconds())) / 1e6)
				}
				logging.Debug("SEND Sent:", f.GetRelPath(), f.GetSize(), f.GetStarted(), f.GetCompleted())
				logging.Sent(f.GetRelPath(), f.GetHash(), f.GetSize(), ms, sender.conf.TargetName)
				done = append(done, f)
			}
		}
		s := b.GetCompleted().Sub(b.GetStarted()).Seconds()
		mb := float64(b.Bytes-b.BytesLeft) / float64(1024) / float64(1024)
		logging.Info(fmt.Sprintf("SEND Bin Throughput: %3d part(s), %10.2f MB, %6.2f sec, %6.2f MB/sec", len(b.Parts), mb, s, mb/s))
		sender.addStats(b.Bytes - b.BytesLeft)
		sender.done(done)
	}
}

// sendBin sends a bin in a loop to handle errors and keep trying.
func (sender *Sender) sendBin(bin *bin.Bin, gz *gzip.Writer) {
	nerr := 0
	for {
		bin.SetStarted()
		n, err := sender.httpBin(bin, gz)
		if err == nil {
			break
		}
		// If somehow part of the bin was successful, let's only resend where we
		// left off.
		if n > 0 && n < len(bin.Parts) {
			b := bin.Split(n)
			bin.SetCompleted()
			sender.ch.doneBin <- bin
			bin = b
		}
		nerr++
		logging.Error("Bin send failed:", err.Error())
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
	bin.SetCompleted()
	sender.ch.doneBin <- bin
}

// done adds the slice of input files to each of the "done" channels.
func (sender *Sender) done(f []sts.SendFile) {
	if len(f) == 0 {
		return
	}
	for _, c := range sender.ch.Done {
		c <- f
	}
}

// httpBin sends the input bin via http using the sender configuration struct info.
// Returns the number of successfully received parts and any error encountered.
func (sender *Sender) httpBin(b *bin.Bin, gz *gzip.Writer) (n int, err error) {
	br := bin.NewEncoder(b)
	jr, err := httputil.GetJSONReader(br.Meta, gzip.NoCompression)
	if err != nil {
		return
	}
	// We have to read it all into memory in order to calculate the length so the
	// receiving end knows how much of the beginning of the payload belongs to
	// metadata.
	var meta []byte
	if meta, err = ioutil.ReadAll(jr); err != nil {
		return
	}
	mr := bytes.NewReader(meta)
	pr, pw := io.Pipe()
	go func() {
		if gz != nil {
			gz.Reset(pw)
			io.Copy(gz, mr)
			io.Copy(gz, br)
			gz.Close()
		} else {
			io.Copy(pw, mr)
			io.Copy(pw, br)
		}
		pw.Close()
	}()
	url := fmt.Sprintf("%s://%s/data", sender.conf.Protocol(), sender.conf.TargetHost)
	req, err := http.NewRequest("PUT", url, pr)
	if err != nil {
		return
	}
	req.Header.Add(httputil.HeaderSourceName, sender.conf.SourceName)
	req.Header.Add(httputil.HeaderMetaLen, strconv.Itoa(len(meta)))
	req.Header.Add(httputil.HeaderSep, string(os.PathSeparator))
	if sender.conf.TargetKey != "" {
		req.Header.Add(httputil.HeaderKey, sender.conf.TargetKey)
	}
	if gz != nil {
		req.Header.Add("Content-Encoding", "gzip")
	}
	resp, err := sender.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPartialContent {
		n, _ = strconv.Atoi(resp.Header.Get(httputil.HeaderPartCount))
		err = fmt.Errorf("Bin failed validation. Successful part(s): %s", resp.Header.Get(httputil.HeaderPartCount))
		return
	} else if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bin failed with response code: %d", resp.StatusCode)
		return
	}
	n = len(b.Parts)
	return
}

func (sender *Sender) addStats(bytes int64) {
	sender.statLock.Lock()
	defer sender.statLock.Unlock()
	sender.bytesOut += bytes
	d := time.Now().Sub(sender.statSince)
	if d > sender.conf.StatInterval {
		s := d.Seconds()
		mb := float64(sender.bytesOut) / float64(1024) / float64(1024)
		logging.Info(fmt.Sprintf("SEND Throughput: %.2fMB, %.2fs, %.2f MB/s", mb, s, mb/s))
		sender.bytesOut = 0
		sender.statSince = time.Now()
	}
}