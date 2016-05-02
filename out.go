package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
)

// AppOut is the struct container for the outgoing portion of the STS app.
type AppOut struct {
	root     string
	dirConf  *OutDirs
	rawConf  *OutSource
	conf     *SenderConf
	once     bool
	scanChan chan []ScanFile
	sortChan chan SortFile
	loopChan chan []SendFile
	pollChan chan []SendFile
	doneChan []chan []DoneFile
	scanner  *Scanner
	sorter   *Sorter
	sender   *Sender
	poller   *Poller
}

func (a *AppOut) setDefaults() {
	if a.conf.BinSize == 0 {
		a.conf.BinSize = 10 * 1024 * 1024 * 1024
	}
	if a.conf.Timeout == 0 {
		a.conf.Timeout = 3600 * 60
	}
	if a.conf.PollAttempts == 0 {
		a.conf.PollAttempts = 10
	}
	if a.conf.PollInterval == 0 {
		a.conf.PollInterval = 60
	}
	if a.conf.PollDelay == 0 {
		a.conf.PollDelay = 5
	}
}

func (a *AppOut) initConf() {
	a.conf = &SenderConf{
		Threads:      a.rawConf.Threads,
		Compression:  a.rawConf.Compression,
		SourceName:   a.rawConf.Name,
		TargetName:   a.rawConf.Target.Name,
		TargetHost:   a.rawConf.Target.Host,
		TargetKey:    a.rawConf.Target.Key,
		TLSCert:      a.rawConf.Target.TLSCert,
		TLSKey:       a.rawConf.Target.TLSKey,
		BinSize:      a.rawConf.BinSize,
		Timeout:      a.rawConf.Timeout,
		StatInterval: a.rawConf.StatInterval,
		PollInterval: a.rawConf.PollInterval,
		PollDelay:    a.rawConf.PollDelay,
		PollAttempts: a.rawConf.PollAttempts,
	}
	if a.conf.SourceName == "" {
		panic("Source name missing from configuration")
	}
	if a.conf.TargetName == "" {
		panic("Target name missing from configuration")
	}
	if a.conf.TargetHost == "" {
		panic("Target host missing from configuration")
	}
	if a.rawConf.Target.TLS && (a.conf.TLSCert == "" || a.conf.TLSKey == "") {
		panic("TLS enabled but missing TLS Cert and/or TLS Key")
	}
	if !a.rawConf.Target.TLS {
		a.conf.TLSCert = ""
		a.conf.TLSKey = ""
	}
	if a.conf.StatInterval.Nanoseconds() == 0 {
		a.conf.StatInterval = time.Minute * 5
	}
	if a.rawConf.GroupBy.String() == "" {
		a.rawConf.GroupBy, _ = regexp.Compile(`^([^\.]*)`) // Default is up to the first dot of the relative path.
	}
	for _, p := range []*string{&a.conf.TLSCert, &a.conf.TLSKey} {
		if *p == "" {
			continue
		}
		if !filepath.IsAbs(*p) {
			*p = filepath.Join(a.root, *p)
		}
		if _, err := os.Stat(*p); os.IsNotExist(err) {
			panic("TLS enabled but cannot find TLS Cert and/or TLS Key")
		}
	}
	a.setDefaults()
}

func (a *AppOut) initComponents() (err error) {
	a.scanChan = make(chan []ScanFile, 1) // One batch at a time.
	a.sortChan = make(chan SortFile, a.rawConf.Threads*2)
	a.loopChan = make(chan []SendFile, a.rawConf.Threads*2)
	a.pollChan = make(chan []SendFile, a.rawConf.Threads*2)
	a.doneChan = []chan []DoneFile{
		make(chan []DoneFile, a.rawConf.Threads*2),
		make(chan []DoneFile, a.rawConf.Threads*2),
	}

	outDir := InitPath(a.root, filepath.Join(a.dirConf.Out, a.rawConf.Target.Name), true)
	cacheDir := InitPath(a.root, a.dirConf.Cache, true)

	scanConf := ScannerConf{
		ScanDir:  outDir,
		CacheDir: cacheDir,
		Delay:    time.Second * 10,
		MinAge:   a.rawConf.MinAge,
		MaxAge:   a.rawConf.MaxAge,
		OutOnce:  true,
		Nested:   0,
	}

	a.scanner = NewScanner(&scanConf)
	a.sorter = NewSorter(a.rawConf.Tags, a.rawConf.GroupBy)
	if a.sender, err = NewSender(a.conf); err != nil {
		return
	}
	a.poller = NewPoller(a.conf)

	for _, tag := range a.rawConf.Tags {
		if tag.Method == MethodHTTP {
			continue
		}
		if tag.Method == MethodNone || tag.Method == MethodDisk {
			scanConf.AddIgnore(tag.Pattern)
		}
	}
	return
}

func (a *AppOut) getPartials() ([]*Companion, error) {
	var err error
	var client *http.Client
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser
	if client, err = httputils.GetClient(a.conf.TLSCert, a.conf.TLSKey); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s://%s/partials", a.conf.Protocol(), a.conf.TargetHost)
	if req, err = http.NewRequest("GET", url, bytes.NewReader([]byte(""))); err != nil {
		return nil, err
	}
	req.Header.Add(httputils.HeaderSourceName, a.conf.SourceName)
	if a.conf.TargetKey != "" {
		req.Header.Add(httputils.HeaderKey, a.conf.TargetKey)
	}
	if resp, err = client.Do(req); err != nil {
		return nil, err
	}
	if reader, err = httputils.GetRespReader(resp); err != nil {
		return nil, err
	}
	defer reader.Close()
	partials := []*Companion{}
	jsonDecoder := json.NewDecoder(reader)
	err = jsonDecoder.Decode(&partials)
	if err != nil {
		return nil, err
	}
	return partials, nil
}

func (a *AppOut) getRecoverable() (send []ScanFile, poll []PollFile, err error) {
	files := a.scanner.GetScanFiles()
	partials, err := a.getPartials()
	if err != nil {
		return nil, nil, err
	}
	pmap := make(map[string]*Companion)
	for _, cmp := range partials {
		pmap[cmp.Path] = cmp
	}
	fmap := make(map[string]ScanFile)
	for _, file := range files {
		changed, err := file.Reset()
		if err != nil {
			logging.Error("Failed to stat partially sent file:", file.GetPath(false), err.Error())
			continue
		}
		if changed {
			logging.Debug("RECOVER Ignore Changed File:", file.GetPath(false))
			continue
		}
		cmp, found := pmap[file.GetRelPath()]
		if found {
			// Partially sent.  Need to gracefully recover.
			rf := &partialFile{
				file:  file,
				hash:  cmp.Hash,
				prev:  cmp.PrevFile,
				alloc: [][2]int64{},
				sent:  0,
			}
			for _, p := range cmp.Parts {
				rf.addAllocPart(p.Beg, p.End)
				rf.AddSent(p.End - p.Beg)
				logging.Debug("RECOVER Found Partial", rf.GetRelPath(), p.Beg, p.End, rf.IsSent())
			}
			send = append(send, rf)

		} else {
			logging.Debug("RECOVER Found Poll", file.GetRelPath())
			// Either fully sent or not at all.  Need to poll to find out which.
			poll = append(poll, &askFile{
				file:    file,
				start:   time.Unix(file.GetTime(), 0),
				success: true,
			})
		}
		fmap[file.GetRelPath()] = file
	}
	for _, cmp := range pmap {
		_, found := fmap[cmp.Path]
		if !found {
			logging.Error("Stale companion file found:", a.conf.TargetName, cmp.Path)
		}
	}
	return
}

func (a *AppOut) recover() {
	// Attempt to recover any partially or fully sent files that weren't confirmed before last shutdown.
	send, poll, err := a.getRecoverable()
	if err != nil {
		panic(err.Error())
	}
	// Poll files need to be polled just once before moving on.
	if len(poll) > 0 {
		none, fail, done, err := a.poller.Poll(poll)
		if err != nil {
			panic(err.Error())
		}
		// It's important that we get these files in the same batch as the recoverable
		// so that the file order stays intact.  Otherwise we might get a circular
		// dependency since the recover group might include files that were meant to
		// be put away after some of these that haven't been partially sent yet.
		for _, f := range append(none, fail...) {
			send = append(send, f.GetOrigFile().(ScanFile))
		}
		if len(done) > 0 {
			// The scanner may not process the "done" list before sending files out
			// so we need to mark them as already queued.
			var d []DoneFile
			for _, f := range done {
				logging.Debug("RECOVER Found Already Done:", f.(DoneFile).GetRelPath())
				a.scanner.SetQueued(f.(DoneFile).GetPath()) // So they don't get added again.
				d = append(d, f.(DoneFile))
				if !logging.FindSent(f.GetRelPath(), f.GetStarted(), time.Now(), a.conf.TargetName) {
					// Log that it was sent if crashed between being sent and being logged.
					logging.Sent(f.GetRelPath(), "", f.GetOrigFile().(ScanFile).GetSize(), 0, a.conf.TargetName)
				}
			}
			for _, dc := range a.doneChan {
				dc <- d
			}
		}
	}
	if len(send) > 0 {
		for _, f := range send {
			a.scanner.SetQueued(f.GetPath(false)) // So they don't get added again.
		}
		a.scanChan <- send // Sneakily insert these files into the send queue ahead of everything else.
	}
}

// Start initializes and starts the outbound components.
// If the "stop" channel is nil then we should only run once (on a single scan).
func (a *AppOut) Start(stop <-chan bool) <-chan bool {
	if stop == nil {
		a.once = true // Not really using this yet but seemed like a good idea.
	}

	// Validate configuration and set defaults.
	a.initConf()

	// Initialize channels and app components.
	if err := a.initComponents(); err != nil {
		panic(err.Error())
	}

	// Recover any unfinished business from last run.
	a.recover()

	var wg sync.WaitGroup
	wg.Add(4) // One for each component.

	// Need to make a separate array for the write-only references to the done channels sent to the poller.
	var dc []chan<- []DoneFile
	for _, c := range a.doneChan {
		dc = append(dc, c)
	}

	pollStop := make(chan bool)

	// Start the poller.
	go func(poller *Poller, in <-chan []SendFile, fail chan<- []SendFile, stop <-chan bool, out ...chan<- []DoneFile) {
		defer wg.Done()
		poller.Start(&PollerChan{
			In:   in,
			Fail: fail,
			Done: out,
			Stop: stop,
		})
	}(a.poller, a.pollChan, a.loopChan, pollStop, dc...)

	// Start the sender.
	go func(sender *Sender, in <-chan SortFile, loop <-chan []SendFile, close chan<- bool, out ...chan<- []SendFile) {
		defer wg.Done()
		sender.Start(&SenderChan{
			In:    in,
			Retry: loop,
			Done:  out,
			Close: close,
		})
	}(a.sender, a.sortChan, a.loopChan, pollStop, a.pollChan)

	// Start the sorter.
	go func(sorter *Sorter, in <-chan []ScanFile, out chan<- SortFile, done <-chan []DoneFile) {
		defer wg.Done()
		sorter.Start(in, out, done)
	}(a.sorter, a.scanChan, a.sortChan, a.doneChan[0])

	// Start the scanner.
	var scanStop chan bool
	if stop != nil {
		scanStop = make(chan bool)
	}
	go func(scanner *Scanner, out chan<- []ScanFile, done <-chan []DoneFile, stop <-chan bool) {
		defer wg.Done()
		scanner.Start(out, done, stop)
	}(a.scanner, a.scanChan, a.doneChan[1], scanStop)

	// Ready.
	logging.Debug(fmt.Sprintf("SENDER Ready: %s -> %s", a.conf.SourceName, a.conf.TargetName))

	done := make(chan bool)
	go func(a *AppOut, stop <-chan bool, done chan<- bool) {
		if stop != nil {
			<-stop // Block until we're told to stop.
		}
		if scanStop != nil {
			close(scanStop) // Start the chain reaction to stop.
		}
		wg.Wait()   // Wait for all goroutines to return.
		close(done) // Close the done channel to let caller know we're all done.
	}(a, stop, done)

	return done
}

// askFile needs to implement PollFile and DoneFile
type askFile struct {
	file    ScanFile
	start   time.Time
	success bool
}

func (f *askFile) GetPath() string {
	return f.file.GetPath(false)
}

func (f *askFile) GetRelPath() string {
	return f.file.GetRelPath()
}

func (f *askFile) GetStarted() time.Time {
	return f.start
}

func (f *askFile) GetSuccess() bool {
	return f.success
}

func (f *askFile) GetOrigFile() interface{} {
	return f.file
}

// Implements RecoverFile
type partialFile struct {
	file  ScanFile
	prev  string
	hash  string
	alloc [][2]int64
	sent  int64
}

func (p *partialFile) GetPath(follow bool) string {
	return p.file.GetPath(follow)
}

func (p *partialFile) GetRelPath() string {
	return p.file.GetRelPath()
}

func (p *partialFile) GetSize() int64 {
	return p.file.GetSize()
}

func (p *partialFile) GetTime() int64 {
	return p.file.GetTime()
}

func (p *partialFile) GetHash() string {
	return p.hash
}

func (p *partialFile) GetNextAlloc() (int64, int64) {
	b := int64(0)
	for i := 0; i < len(p.alloc); i++ {
		if b < p.alloc[i][0] {
			return b, p.alloc[i][0]
		}
		b = p.alloc[i][1]
	}
	return b, p.file.GetSize()
}

func (p *partialFile) GetBytesAlloc() int64 {
	b := int64(0)
	for i := 0; i < len(p.alloc); i++ {
		b += p.alloc[i][1] - p.alloc[i][0]
	}
	return b - p.sent
}

func (p *partialFile) GetBytesSent() int64 {
	return p.sent
}

func (p *partialFile) AddAlloc(bytes int64) {
	b, _ := p.GetNextAlloc()
	p.addAllocPart(b, b+bytes)
}

func (p *partialFile) addAllocPart(beg int64, end int64) {
	if len(p.alloc) == 0 || beg >= p.alloc[len(p.alloc)-1][1] {
		p.alloc = append(p.alloc, [2]int64{beg, end})
		return
	}
	for i := 0; i < len(p.alloc); i++ {
		// SJB: Important we don't expand/change allocated blocks because then we will get out of sync
		// with the already-existing companion file on the receiving end.
		if beg < p.alloc[i][0] {
			if i > 0 {
				// https://github.com/golang/go/wiki/SliceTricks (Insert)
				p.alloc = append(p.alloc[:i], append([][2]int64{[2]int64{beg, end}}, p.alloc[i:]...)...)
			} else {
				// Prepend (unshift)
				p.alloc = append([][2]int64{[2]int64{beg, end}}, p.alloc...)
			}
			return
		}
	}
}

func (p *partialFile) AddSent(b int64) {
	p.sent += b
}

func (p *partialFile) IsSent() bool {
	return p.sent == p.file.GetSize()
}

func (p *partialFile) Reset() (changed bool, err error) {
	changed, err = p.file.Reset()
	if changed {
		p.hash, err = fileutils.FileMD5(p.file.GetPath(true))
	}
	return
}
