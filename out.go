package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	dirConf  *SendDirs
	rawConf  *SendSource
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
	if a.conf.MaxRetries == 0 {
		a.conf.MaxRetries = 10
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
		Compress:     a.rawConf.Compress,
		SourceName:   a.rawConf.Name,
		TargetName:   a.rawConf.Target.Name,
		TargetHost:   a.rawConf.Target.Host,
		TLSCert:      a.rawConf.Target.TLSCert,
		TLSKey:       a.rawConf.Target.TLSKey,
		BinSize:      a.rawConf.BinSize,
		Timeout:      a.rawConf.Timeout,
		MaxRetries:   a.rawConf.MaxRetries,
		PollInterval: a.rawConf.PollInterval,
		PollDelay:    a.rawConf.PollDelay,
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
		panic("TLS enabled but missing either TLSCert or TLSKey")
	}
	if a.rawConf.GroupBy.String() == "" {
		a.rawConf.GroupBy, _ = regexp.Compile(`^([^\.]*)`) // Default is up to the first dot of the relative path.
	}
	a.setDefaults()
}

func (a *AppOut) initComponents() {
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

	a.scanner = NewScanner(outDir, cacheDir, time.Second*5, a.rawConf.MinAge, true, 0)
	a.sorter = NewSorter(a.rawConf.Tags, a.rawConf.GroupBy)
	a.sender = NewSender(a.conf)
	a.poller = NewPoller(a.conf)

	for _, tag := range a.rawConf.Tags {
		if tag.Method == MethodHTTP {
			continue
		}
		if tag.Method == MethodNone || tag.Method == MethodDisk {
			a.scanner.AddIgnore(tag.Pattern)
		}
	}
}

func (a *AppOut) getPartials() ([]*Companion, error) {
	var err error
	var client *http.Client
	var req *http.Request
	var resp *http.Response
	if client, err = httputils.GetClient(a.conf.TLSCert, a.conf.TLSKey); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s://%s/partials", a.conf.Protocol(), a.conf.TargetHost)
	if req, err = http.NewRequest("GET", url, bytes.NewReader([]byte(""))); err != nil {
		return nil, err
	}
	req.Header.Add(HeaderSourceName, a.conf.SourceName)
	if resp, err = client.Do(req); err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	partials := []*Companion{}
	jsonDecoder := json.NewDecoder(resp.Body)
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
		// Make sure size/time still match.  If not, just need to send the whole thing again.
		info, err := os.Stat(file.GetPath())
		if err != nil {
			logging.Error("Failed to stat partially sent file:", file.GetPath(), err.Error())
			continue
		}
		if info.Size() != file.GetSize() || info.ModTime().Unix() != file.GetTime() {
			logging.Debug("RECOVER Ignore Changed File:", file.GetPath())
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
			// Either fully sent or not at all.  Need to poll to find out which.
			poll = append(poll, &askFile{
				path:    file.GetPath(),
				relPath: file.GetRelPath(),
				start:   file.GetTime() * 1e9, // Expects nanoseconds
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
	var done []DoneFile
	if len(poll) > 0 {
		_, done, err = a.poller.Poll(poll)
		if err != nil {
			panic(err.Error())
		}
		if len(done) > 0 {
			logging.Debug("MAIN Found Already Done:", len(done))
			for _, dc := range a.doneChan {
				dc <- done // The scanner will get this first before picking them up to send again.
			}
		}
	}
	if len(send) > 0 {
		for _, f := range send {
			a.scanner.SetQueued(f.GetPath()) // So they don't get added again.
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
	a.initComponents()

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

// Implements PollFile and DoneFile
type askFile struct {
	path    string
	relPath string
	start   int64
}

func (f *askFile) GetPath() string {
	return f.path
}

func (f *askFile) GetRelPath() string {
	return f.relPath
}

func (f *askFile) GetStarted() int64 {
	return f.start
}

// Implements RecoverFile
type partialFile struct {
	file  ScanFile
	prev  string
	hash  string
	alloc [][2]int64
	sent  int64
}

func (p *partialFile) GetPath() string {
	return p.file.GetPath()
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

func (p *partialFile) GetPrevName() string {
	return p.prev
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
		p.hash, err = fileutils.FileMD5(p.file.GetPath())
	}
	return
}
