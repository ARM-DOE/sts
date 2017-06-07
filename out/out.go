package out

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/companion"
	"code.arm.gov/dataflow/sts/conf"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/httputil"
	"code.arm.gov/dataflow/sts/logging"
	"code.arm.gov/dataflow/sts/poll"
	"code.arm.gov/dataflow/sts/scan"
	"code.arm.gov/dataflow/sts/send"
	"code.arm.gov/dataflow/sts/sort"
	"code.arm.gov/dataflow/sts/util"
)

// AppOut is the struct container for the outgoing portion of the STS app.
type AppOut struct {
	Root     string
	DirConf  *conf.OutDirs
	RawConf  *conf.OutSource
	conf     *send.SenderConf
	once     bool
	scanChan chan []sts.ScanFile
	sortChan map[string]chan sts.SortFile
	loopChan chan []sts.SendFile
	pollChan chan []sts.SendFile
	doneChan []chan []sts.DoneFile
	Scanner  *scan.Scanner
	Sorter   *sort.Sorter
	Sender   *send.Sender
	Poller   *poll.Poller
}

func (a *AppOut) setDefaults() {
	if a.DirConf.Cache == "" {
		a.DirConf.Cache = ".sts"
	}
	if a.DirConf.Logs == "" {
		a.DirConf.Logs = "logs"
	}
	if a.DirConf.Out == "" {
		a.DirConf.Out = "out"
	}
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
	if a.conf.StatInterval == 0 {
		a.conf.StatInterval = time.Minute * 5
	}
	if a.RawConf.CacheAge == 0 {
		a.RawConf.CacheAge = a.conf.StatInterval
	}
	if a.RawConf.ScanDelay == 0 {
		a.RawConf.ScanDelay = time.Second * 30
	}
	if a.RawConf.GroupBy == nil || a.RawConf.GroupBy.String() == "" {
		a.RawConf.GroupBy = regexp.MustCompile(`^([^\.]*)`) // Default is up to the first dot of the relative path.
	}
}

func (a *AppOut) initConf() {
	a.conf = &send.SenderConf{
		Threads:      a.RawConf.Threads,
		Compression:  a.RawConf.Compression,
		SourceName:   a.RawConf.Name,
		TargetName:   a.RawConf.Target.Name,
		TargetHost:   a.RawConf.Target.Host,
		TargetKey:    a.RawConf.Target.Key,
		BinSize:      a.RawConf.BinSize,
		Timeout:      a.RawConf.Timeout,
		StatInterval: a.RawConf.StatInterval,
		PollInterval: a.RawConf.PollInterval,
		PollDelay:    a.RawConf.PollDelay,
		PollAttempts: a.RawConf.PollAttempts,
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
	if a.RawConf.Target.TLSCertPath != "" {
		var err error
		certPath := util.InitPath(a.Root, a.RawConf.Target.TLSCertPath, false)
		if a.conf.TLS, err = httputil.GetTLSConf("", "", certPath); err != nil {
			panic(err)
		}
	}
	a.setDefaults()
}

func (a *AppOut) initComponents() (err error) {
	a.scanChan = make(chan []sts.ScanFile, 1) // One batch at a time.
	a.sortChan = map[string]chan sts.SortFile{
		conf.MethodHTTP: make(chan sts.SortFile, a.RawConf.Threads*2),
	}
	a.loopChan = make(chan []sts.SendFile, a.RawConf.Threads*2)
	a.pollChan = make(chan []sts.SendFile, a.RawConf.Threads*2)
	a.doneChan = []chan []sts.DoneFile{
		make(chan []sts.DoneFile, a.RawConf.Threads*2),
		make(chan []sts.DoneFile, a.RawConf.Threads*2),
	}

	var outDir string
	if a.RawConf.OutDir != "" {
		outDir = util.InitPath(a.Root, a.RawConf.OutDir, true)
	} else {
		outDir = util.InitPath(a.Root, filepath.Join(a.DirConf.Out, a.RawConf.Target.Name), true)
	}
	cacheDir := util.InitPath(a.Root, a.DirConf.Cache, true)

	scanConf := scan.ScannerConf{
		ScanDir:        outDir,
		CacheDir:       cacheDir,
		CacheAge:       a.RawConf.CacheAge,
		Delay:          a.RawConf.ScanDelay,
		MinAge:         a.RawConf.MinAge,
		MaxAge:         a.RawConf.MaxAge,
		OutOnce:        true,
		Nested:         0,
		Include:        a.RawConf.Include,
		Ignore:         a.RawConf.Ignore,
		FollowSymlinks: a.DirConf.OutFollow,
		ZeroError:      true,
	}

	if a.Scanner, err = scan.NewScanner(&scanConf); err != nil {
		return
	}
	var tags []*conf.Tag
	for _, tag := range a.RawConf.Tags {
		switch tag.Method {
		case conf.MethodHTTP:
			tags = append(tags, tag)
		default:
			// Ignore unknown methods.
			// This allows us to extend the configuration for outside uses like
			// disk mode, which is not part of this package.
			scanConf.AddIgnore(tag.Pattern)
		}
	}

	a.Sorter = sort.NewSorter(tags, a.RawConf.GroupBy)
	if a.Sender, err = send.NewSender(a.conf); err != nil {
		return
	}
	a.Poller = poll.NewPoller(a.conf)
	return
}

func (a *AppOut) getPartials() ([]*companion.Companion, error) {
	var err error
	var client *http.Client
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser
	if client, err = httputil.GetClient(a.conf.TLS); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s://%s/partials", a.conf.Protocol(), a.conf.TargetHost)
	if req, err = http.NewRequest("GET", url, bytes.NewReader([]byte(""))); err != nil {
		return nil, err
	}
	req.Header.Add(httputil.HeaderSourceName, a.conf.SourceName)
	if a.conf.TargetKey != "" {
		req.Header.Add(httputil.HeaderKey, a.conf.TargetKey)
	}
	if resp, err = client.Do(req); err != nil {
		return nil, err
	}
	if reader, err = httputil.GetRespReader(resp); err != nil {
		return nil, err
	}
	defer reader.Close()
	partials := []*companion.Companion{}
	jsonDecoder := json.NewDecoder(reader)
	err = jsonDecoder.Decode(&partials)
	if err != nil {
		return nil, err
	}
	return partials, nil
}

func (a *AppOut) getRecoverable() (send []sts.ScanFile, poll []sts.PollFile, err error) {
	files := a.Scanner.GetScanFiles()
	partials, err := a.getPartials()
	if err != nil {
		return nil, nil, err
	}
	pmap := make(map[string]*companion.Companion)
	for _, cmp := range partials {
		pmap[cmp.Path] = cmp
	}
	fmap := make(map[string]sts.ScanFile)
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

// Init creates internal components and initializes configuration.
func (a *AppOut) Init() {

	// Validate configuration and set defaults.
	a.initConf()

	// Initialize channels and app components.
	if err := a.initComponents(); err != nil {
		panic(err.Error())
	}
}

// Recover attempts to recover any partially or fully sent files that weren't
// confirmed before last shutdown.
func (a *AppOut) Recover() error {
	send, poll, err := a.getRecoverable()
	if err != nil {
		return err
	}
	// Poll files need to be polled just once before moving on.
	if len(poll) > 0 {
		none, fail, done, wait, err := a.Poller.Poll(poll)
		if err != nil {
			return err
		}
		// It's important that we get these files in the same batch as the recoverable
		// so that the file order stays intact.  Otherwise we might get a circular
		// dependency since the recover group might include files that were meant to
		// be put away after some of these that haven't been partially sent yet.
		none = append(none, wait...)
		for _, f := range append(none, fail...) {
			send = append(send, f.GetOrigFile().(sts.ScanFile))
		}
		if len(done) > 0 {
			// The scanner may not process the "done" list before sending files out
			// so we need to mark them as already queued.
			var d []sts.DoneFile
			for _, f := range done {
				logging.Debug("RECOVER Found Already Done:", f.(sts.DoneFile).GetRelPath())
				a.Scanner.SetQueued(f.(sts.DoneFile).GetPath()) // So they don't get added again.
				d = append(d, f.(sts.DoneFile))
				if !logging.FindSent(f.GetRelPath(), f.GetStarted(), time.Now(), a.conf.TargetName) {
					// Log that it was sent if crashed between being sent and being logged.
					logging.Sent(f.GetRelPath(), "", f.GetOrigFile().(sts.ScanFile).GetSize(), 0, a.conf.TargetName)
				}
			}
			for _, dc := range a.doneChan {
				dc <- d
			}
		}
	}
	if len(send) > 0 {
		for _, f := range send {
			a.Scanner.SetQueued(f.GetPath(false)) // So they don't get added again.
		}
		a.scanChan <- send // Sneakily insert these files into the send queue ahead of everything else.
	}
	return nil
}

// Start initializes and starts the outbound components.
// If the "stop" channel is nil then we should only run once (on a single scan).
func (a *AppOut) Start(stop <-chan bool) <-chan bool {
	if stop == nil {
		a.once = true // Not really using this yet but seemed like a good idea.
	}

	var wg sync.WaitGroup
	wg.Add(4) // One for each component.

	// Need to make a separate array for the write-only references to the done channels sent to the poller.
	var dc []chan<- []sts.DoneFile
	for _, c := range a.doneChan {
		dc = append(dc, c)
	}

	pollStop := make(chan bool)

	// Start the poller.
	go func(poller *poll.Poller, in <-chan []sts.SendFile, fail chan<- []sts.SendFile, stop <-chan bool, out ...chan<- []sts.DoneFile) {
		defer wg.Done()
		poller.Start(&poll.PollerChan{
			In:   in,
			Fail: fail,
			Done: out,
			Stop: stop,
		})
	}(a.Poller, a.pollChan, a.loopChan, pollStop, dc...)

	// Start the sender.
	go func(sender *send.Sender, in <-chan sts.SortFile, loop <-chan []sts.SendFile, close chan<- bool, out ...chan<- []sts.SendFile) {
		defer wg.Done()
		sender.Start(&send.SenderChan{
			In:    in,
			Retry: loop,
			Done:  out,
			Close: close,
		})
	}(a.Sender, a.sortChan[conf.MethodHTTP], a.loopChan, pollStop, a.pollChan)

	// Start the sorter.
	go func(sorter *sort.Sorter, in <-chan []sts.ScanFile, out map[string]chan sts.SortFile, done <-chan []sts.DoneFile) {
		defer wg.Done()
		sorter.Start(in, out, done)
	}(a.Sorter, a.scanChan, a.sortChan, a.doneChan[0])

	// Start the scanner.
	var scanStop chan bool
	if stop != nil {
		scanStop = make(chan bool)
	}
	go func(scanner *scan.Scanner, out chan<- []sts.ScanFile, done <-chan []sts.DoneFile, stop <-chan bool) {
		defer wg.Done()
		scanner.Start(out, done, stop)
	}(a.Scanner, a.scanChan, a.doneChan[1], scanStop)

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
	file    sts.ScanFile
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

// partialFile implements RecoverFile
type partialFile struct {
	file  sts.ScanFile
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
	p.alloc = nil
	p.sent = 0
	changed, err = p.file.Reset()
	if changed {
		p.hash, err = fileutil.FileMD5(p.file.GetPath(true))
	}
	return
}

func (p *partialFile) Stat() (changed bool, err error) {
	return p.file.Reset()
}
