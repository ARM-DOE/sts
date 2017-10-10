package client

import (
	"sort"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/log"
	"github.com/alecthomas/units"
)

// Conf is the struct for storing all the settable components and options for
// this client to manage the outgoing data flow
type Conf struct {
	// Components
	Store        sts.FileSource     // The source of file objects from which to read (and delete)
	Cache        sts.FileCache      // The persistent cache of file metadata from the store
	Queue        sts.FileQueue      // The queue that takes files ready to send and chunks them up in its configured order
	Recoverer    sts.Recover        // The function that determines what chunks need to be sent first
	BuildPayload sts.PayloadFactory // The function that creates a Payload instance
	Transmitter  sts.Transmit       // The transmission implementation
	Validator    sts.Validate       // The validation implementation
	Logger       sts.SendLogger     // The logger used to log completed files

	// Helpers
	Tagger sts.Translate // Takes a file name and provides a tag name

	// Options
	CacheAge     time.Duration    // How long to keep files in the cache after being sent (only applies to files that aren't deleted after being sent)
	ScanDelay    time.Duration    // How long to wait between scans of the store (0 means only once and we're done)
	Threads      int              // How many go routines for sending data
	PayloadSize  units.Base2Bytes // How large a single transmission payload should be (before any compression by the transmitter)
	StatInterval time.Duration    // How long to wait between logging transmission rate statistics
	PollDelay    time.Duration    // How long to wait before validating completed files after final transmission
	PollInterval time.Duration    // How long to wait between between validation requests
	PollAttempts int              // How many times to attempt validation for a single file before giving up
	Tags         []*FileTag       // Options for files of a particular pattern
}

// FileTag is the struct for defining settings relevant to files of a "tag"
// that are relevant at the client level
type FileTag struct {
	Name    string // The name of the tag to be matched by the tagger
	InOrder bool   // Whether or not the order files are received matters
	Delete  bool
}

// Broker is the client struct
type Broker struct {
	Conf      *Conf
	tagMap    map[string]*FileTag
	cleanAll  bool
	cleanSome bool

	// Channels
	chStop        chan bool
	chScanned     chan []sts.Hashed
	chQueued      chan sts.Sendable
	chTransmit    chan sts.Payload
	chTransmitted chan sts.Payload
	chValidate    chan sts.Pollable
}

// Start runs the client
func (broker *Broker) Start(stop <-chan bool, done chan<- bool) {
	broker.tagMap = make(map[string]*FileTag)
	broker.cleanAll = true
	for _, tag := range broker.Conf.Tags {
		broker.tagMap[tag.Name] = tag
		broker.cleanAll = broker.cleanAll && tag.Delete
		broker.cleanSome = broker.cleanSome || tag.Delete
	}

	broker.chStop = make(chan bool)
	broker.chScanned = make(chan []sts.Hashed, 1)
	broker.chQueued = make(chan sts.Sendable, broker.Conf.Threads*2)
	broker.chTransmit = make(chan sts.Payload, broker.Conf.Threads*2)
	broker.chTransmitted = make(chan sts.Payload, broker.Conf.Threads*2)
	broker.chValidate = make(chan sts.Pollable, broker.Conf.Threads*2)

	send, poll, err := broker.recover()
	if err != nil {
		log.Error("Recovery failed:", err.Error())
		return
	}

	var wg sync.WaitGroup

	go broker.startQueue(&wg)
	go broker.startBin(&wg)
	wg.Add(3)
	for i := 0; i < broker.Conf.Threads; i++ {
		go broker.startSend(&wg)
		wg.Add(1)
	}
	go broker.startTrack(&wg)
	go broker.startValidate(&wg)
	wg.Add(2)

	if len(send) > 0 {
		broker.chScanned <- send
	}
	if len(poll) > 0 {
		for _, p := range poll {
			broker.chValidate <- p
		}
	}

	go broker.startScan(&wg)
	wg.Add(1)

	broker.chStop <- <-stop
	close(broker.chStop)
	wg.Wait()
	done <- true
}

func (broker *Broker) recover() (send []sts.Hashed, poll []sts.Pollable, err error) {
	partials, err := broker.Conf.Recoverer()
	if err != nil {
		return
	}
	lookup := make(map[string]*sts.Partial)
	for _, p := range partials {
		lookup[p.Name] = p
		if broker.Conf.Cache.Get(p.Name) == nil {
			log.Error("Stale companion on target host:", p.Name)
		}
	}
	broker.Conf.Cache.Iterate(func(f sts.Cached) bool {
		var file sts.File
		file, err = broker.Conf.Store.Sync(f)
		if err != nil {
			log.Error("Failed to stat partially sent file:", f.GetPath(), err.Error())
			return false
		}
		if file != nil {
			log.Debug("Ignore changed file:", f.GetPath())
			return false
		}
		partial, exists := lookup[f.GetRelPath()]
		if exists {
			log.Debug("Found partial:", f.GetRelPath())
			// Partially sent; need to gracefully recover
			var parts chunks
			parts = partial.Parts[:0]
			for i, part := range partial.Parts {
				parts[i] = part
			}
			sort.Sort(parts)
			var beg int64
			var missing chunks
			for _, part := range parts {
				if beg == part.Beg {
					beg = part.End
					continue
				}
				missing = append(missing, &sts.ByteRange{
					Beg: beg,
					End: part.Beg,
				})
				beg = part.End
			}
			if beg < f.GetSize() {
				missing = append(missing, &sts.ByteRange{
					Beg: beg,
					End: f.GetSize(),
				})
			}
			send = append(send, &recovered{
				path: f.GetPath(),
				name: f.GetRelPath(),
				prev: partial.Prev,
				size: f.GetSize(),
				time: f.GetTime(),
				hash: f.GetHash(),
				left: missing,
			})
		} else {
			log.Debug("Found file to poll:", f.GetRelPath())
			// Either fully sent or not at all; need to poll to find out which
			poll = append(poll, &progressFile{
				name:      f.GetRelPath(),
				completed: time.Unix(f.GetTime(), 0),
				size:      f.GetSize(),
				hash:      f.GetHash(),
			})
		}
		return false
	})
	if len(poll) > 0 {
		var polled []sts.Polled
		if polled, err = broker.Conf.Validator(poll); err != nil {
			return
		}
		poll = nil
		for _, f := range polled {
			cached := broker.Conf.Cache.Get(f.GetName())
			switch {
			case f.NotFound():
				send = append(send, cached)
				break
			case f.Waiting():
				poll = append(poll, &progressFile{
					name:      cached.GetRelPath(),
					completed: time.Unix(cached.GetTime(), 0),
					size:      cached.GetSize(),
					hash:      cached.GetHash(),
				})
				break
			case f.Failed():
				send = append(send, cached)
				break
			case f.Received():
				if !broker.Conf.Logger.WasSent(f.GetName(), time.Unix(cached.GetTime(), 0), time.Now()) {
					broker.Conf.Logger.Sent(&progressFile{
						name:      cached.GetRelPath(),
						completed: time.Unix(cached.GetTime(), 0),
						size:      cached.GetSize(),
						hash:      cached.GetHash(),
					})
				}
				broker.finish(f)
				break
			}
		}
	}
	err = nil
	return
}

func (broker *Broker) startScan(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		found := broker.scan()
		if len(found) > 0 {
			broker.chScanned <- found
		}
		select {
		case <-broker.chStop:
			close(broker.chScanned)
			return
		default:
			break
		}
		time.Sleep(broker.Conf.ScanDelay)
	}
}

func (broker *Broker) scan() []sts.Hashed {
	var err error
	var files []sts.File
	var scanTime time.Time

	store := broker.Conf.Store
	cache := broker.Conf.Cache

	// For looking up what is already in the cache
	getter := func(key string) sts.File {
		cached := cache.Get(key)
		if cached != nil {
			return cached.(sts.File)
		}
		return nil
	}

	// Perform the scan
	if files, scanTime, err = store.Scan(getter); err != nil {
		log.Error(err)
		return nil
	}
	if len(files) == 0 {
		return nil
	}
	age := scanTime.Add(-1 * broker.Conf.CacheAge)

	// Wrap the scanned file objects in type that can have hash added
	wrapped := make([]sts.Hashed, len(files))
	i := 0
	for _, file := range files {
		if !broker.cleanAll {
			// Based on the tag configuration, exclude files that would have
			// aged off in a scenario where they are not automatically deleted
			// after being sent successfully
			check := true
			if broker.cleanSome {
				tagName := broker.Conf.Tagger(file.GetRelPath())
				if tag, ok := broker.tagMap[tagName]; ok && !tag.Delete {
					check = true
				}
			}
			// Skip files that aren't marked to be cleaned up and that have
			// mod times before the scan time minus the minimum age minus
			// the cache age
			if check && file.GetTime() < age.Unix() {
				continue
			}
		}
		wrapped[i] = &hashFile{
			orig: file,
		}
		i++
	}
	wrapped = wrapped[:i]
	if len(wrapped) == 0 {
		return nil
	}

	// Start the hashing asynchronously
	wg := broker.hash(wrapped, int64(broker.Conf.PayloadSize))

	// Prune the cache
	cache.Iterate(func(cached sts.Cached) bool {
		if cached.GetTime() > age.Unix() || !cached.IsDone() {
			return false
		}
		cache.Remove(cached.GetRelPath())
		return false
	})

	// Wait for all the hashes to be calculated
	wg.Wait()
	for _, file := range wrapped {
		cache.Add(file)
	}
	if err = cache.Persist(); err != nil {
		log.Error(err)
		return nil
	}
	return wrapped
}

func (broker *Broker) hash(scanned []sts.Hashed, batchSize int64) *sync.WaitGroup {
	hashFiles := func(files []sts.Hashed, wg *sync.WaitGroup) {
		wg.Add(1)
		defer wg.Done()
		log.Debug("HASHing %d files...", len(scanned))
		var err error
		var fh sts.Readable
		for _, file := range files {
			log.Debug("HASHing %s ...", file.GetRelPath())
			fh, err = broker.Conf.Store.GetOpener()(file)
			if err != nil {
				log.Error(err)
				return
			}
			defer fh.Close()
			file.(*hashFile).hash, err = fileutil.ReadableMD5(fh)
			if err != nil {
				log.Error(err)
				return
			}
		}
	}
	var size int64
	var wg sync.WaitGroup
	j := 0
	count := 0
	wg.Add(1)
	defer wg.Done()
	for i, file := range scanned {
		size += file.GetSize()
		if size < batchSize {
			count++
			continue
		}
		go hashFiles(scanned[j:j+count], &wg)
		j = i + 1
		count = 0
		size = 0
	}
	if count > 0 {
		go hashFiles(scanned[j:j+count], &wg)
	}
	return &wg
}

func (broker *Broker) startQueue(wg *sync.WaitGroup) {
	defer wg.Done()
	in := broker.chScanned
	out := broker.chQueued
	for {
		select {
		case batch, ok := <-in:
			if !ok {
				in = nil
				return
			}
			broker.Conf.Queue.Add(batch)
		default:
			next := broker.Conf.Queue.GetNext()
			if next == nil {
				if in == nil {
					close(out)
					return
				}
				time.Sleep(time.Millisecond * 500)
				continue
			}
			out <- next
		}
	}
}

func (broker *Broker) startBin(wg *sync.WaitGroup) {
	defer wg.Done()
	var payload sts.Payload
	var sendable sts.Sendable
	var current sts.Binnable
	var ok bool
	wait := time.Millisecond * 500
	waitInterval := wait / 10
	waited := time.Millisecond * 0
	in := broker.chQueued
	out := broker.chTransmit
	for {
		if current == nil {
			select {
			case sendable, ok = <-in:
				if !ok {
					in = nil
					continue
				}
				tagName := broker.Conf.Tagger(sendable.GetRelPath())
				tag := broker.tagMap[tagName]
				current = &binnable{
					orig:   sendable,
					tag:    tagName,
					noPrev: tag == nil || tag.InOrder == false,
				}
			default:
				if in == nil || waited > wait {
					if payload != nil && len(payload.GetParts()) > 0 {
						// Send what we have if we've either waited long enough
						// for more or the input channel is closed and we're
						// done
						out <- payload
						payload = nil
					}
				} else {
					time.Sleep(waitInterval)
					waited += waitInterval
				}
				if in == nil {
					return
				}
				continue
			}
		}
		if payload == nil {
			payload = broker.Conf.BuildPayload(
				int64(broker.Conf.PayloadSize),
				broker.Conf.Store.GetOpener())
		}
		added := payload.Add(current)
		if !added || current.IsAllocated() {
			current = nil
		}
		if payload.IsFull() {
			out <- payload
			payload = nil
		}
	}
}

func (broker *Broker) startSend(wg *sync.WaitGroup) {
	defer wg.Done()
	in := broker.chTransmit
	out := broker.chTransmitted
	nErr := 0
	for {
		payload, ok := <-in
		if !ok {
			break
		}
		for {
			n, err := broker.Conf.Transmitter(payload)
			if err == nil {
				break
			}
			if n > 0 && n < len(payload.GetParts()) {
				next := payload.Split(n)
				out <- payload
				payload = next
			}
			nErr++
			log.Error("Payload send failed:", err.Error())
			// Check each file in the payload to make sure none of them has
			// changed and remove the ones that have
			for _, binned := range payload.GetParts() {
				file := broker.Conf.Cache.Get(binned.GetName())
				if file == nil {
					payload.Remove(binned)
					continue
				}
				if f, err := broker.Conf.Store.Sync(file); f != nil || err != nil {
					// If the file changed, it will get picked up again
					// automatically, so we can just ignore it
					payload.Remove(binned)
				}
			}
			// Wait longer the more it fails
			time.Sleep(time.Duration(nErr) * time.Second)
		}
		out <- payload
	}
}

func (broker *Broker) startTrack(wg *sync.WaitGroup) {
	defer wg.Done()
	in := broker.chTransmitted
	out := broker.chValidate
	progress := make(map[string]*progressFile)
	for {
		payload, ok := <-in
		if !ok {
			break
		}
		for _, binned := range payload.GetParts() {
			key := binned.GetName()
			pFile, ok := progress[key]
			if !ok {
				progress[key] = &progressFile{
					name:    binned.GetName(),
					size:    binned.GetFileSize(),
					started: payload.GetStarted(),
				}
				pFile = progress[key]
			}
			_, n := binned.GetSlice()
			pFile.sent += n
			if pFile.sent >= pFile.size {
				pFile.completed = payload.GetCompleted()
				broker.Conf.Logger.Sent(pFile)
				go func() {
					out <- pFile
				}()
			}
		}
	}
}

func (broker *Broker) startValidate(wg *sync.WaitGroup) {
	defer wg.Done()
	in := broker.chValidate
	poll := make(map[string]*progressFile)
	var pollTime time.Time
	var sent sts.Pollable
	var ready []sts.Pollable
	var polled []sts.Polled
	var ok bool
	var err error
	var nErr int
	for {
		if len(poll) == 0 {
			// If there's nothing in our Q, just block until we get a file
			sent, ok = <-in
			if !ok {
				return
			}
		} else {
			// Don't block if we've got files in the poll Q
			select {
			case sent, ok = <-in:
				if !ok {
					return
				}
			default:
				// To keep from thrashing until it's been long enough
				time.Sleep(time.Millisecond * 100)
				break
			}
		}
		if sent != nil {
			poll[sent.GetName()] = sent.(*progressFile)
			sent = nil
		}
		// Make sure it's been long enough to send another poll
		if !pollTime.IsZero() && time.Now().Sub(pollTime) < broker.Conf.PollInterval {
			continue
		}
		// Build the list of pollable files
		ready = nil
		for k, f := range poll {
			if broker.Conf.PollAttempts > 0 && f.polled > broker.Conf.PollAttempts {
				broker.Conf.Cache.Remove(f.name)
				delete(poll, k)
				continue
			}
			if time.Now().Sub(f.completed) > broker.Conf.PollDelay {
				ready = append(ready, f)
			}
		}
		if len(ready) == 0 {
			// No files ready yet
			continue
		}
		nErr = 0
		for {
			polled, err = broker.Conf.Validator(ready)
			if err != nil {
				log.Error("Poll request failed:", err.Error())
				nErr++
				// Wait longer the more it fails
				time.Sleep(time.Duration(nErr) * time.Second)
				continue
			}
			break
		}
		for _, f := range polled {
			switch {
			case f.NotFound():
				poll[f.GetName()].polled++
				break
			case f.Waiting():
				// Nothing to do with "waiting" files. They should continue to
				// be polled since eventually they will be released on the
				// receiving end
				break
			case f.Failed():
				log.Error("File failed validation:", f.GetName())
				broker.finish(f)
				delete(poll, f.GetName())
				break
			case f.Received():
				broker.finish(f)
				delete(poll, f.GetName())
				break
			}
		}
	}
}

func (broker *Broker) finish(file sts.Polled) {
	switch {
	case file.Received():
		cached := broker.Conf.Cache.Get(file.GetName())
		tagName := broker.Conf.Tagger(file.GetName())
		tag := broker.tagMap[tagName]
		if tag != nil && tag.Delete {
			broker.Conf.Store.Remove(cached)
		}
		fallthrough
	case file.Failed():
		broker.Conf.Cache.Remove(file.GetName())
		break
	}
}

type hashFile struct {
	orig sts.File
	hash string
}

func (f *hashFile) GetPath() string {
	return f.orig.GetPath()
}

func (f *hashFile) GetRelPath() string {
	return f.orig.GetRelPath()
}

func (f *hashFile) GetSize() int64 {
	return f.orig.GetSize()
}

func (f *hashFile) GetTime() int64 {
	return f.orig.GetTime()
}

func (f *hashFile) GetHash() string {
	return f.hash
}

type recovered struct {
	path string
	name string
	prev string
	size int64
	time int64
	hash string
	left []*sts.ByteRange
	part int
	used int64
}

func (f *recovered) GetPath() string {
	return f.path
}

func (f *recovered) GetRelPath() string {
	return f.name
}

func (f *recovered) GetPrev() string {
	return f.prev
}

func (f *recovered) GetSize() int64 {
	return f.size
}

func (f *recovered) GetTime() int64 {
	return f.time
}

func (f *recovered) GetHash() string {
	return f.hash
}

func (f *recovered) Allocate(desired int64) (offset int64, length int64) {
	offset = f.left[f.part].Beg + f.used
	length = desired
	f.used = offset + length
	if offset+length >= f.left[f.part].End {
		length = f.left[f.part].End - offset
		f.part++
		f.used = 0
	}
	return
}

func (f *recovered) IsAllocated() bool {
	return f.part == len(f.left)
}

type chunks []*sts.ByteRange

func (c chunks) Len() int { return len(c) }
func (c chunks) Less(i, j int) bool {
	return c[i].Beg-c[j].Beg < 0
}
func (c chunks) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type binnable struct {
	orig      sts.Sendable
	tag       string
	noPrev    bool
	allocated int64
}

func (f *binnable) GetPath() string {
	return f.orig.GetPath()
}

func (f *binnable) GetRelPath() string {
	return f.orig.GetRelPath()
}

func (f *binnable) GetSize() int64 {
	return f.orig.GetSize()
}

func (f *binnable) GetTime() int64 {
	return f.orig.GetTime()
}

func (f *binnable) GetHash() string {
	return f.orig.GetHash()
}

func (f *binnable) GetPrev() string {
	if f.noPrev {
		return ""
	}
	return f.orig.GetPrev()
}

func (f *binnable) GetSlice() (int64, int64) {
	return f.orig.GetSlice()
}

func (f *binnable) GetNextAlloc() (int64, int64) {
	b, n := f.GetSlice()
	return b + f.allocated, b + n
}

func (f *binnable) AddAlloc(n int64) {
	f.allocated += n
}

func (f *binnable) IsAllocated() bool {
	b, n := f.GetSlice()
	return f.allocated == (b + n)
}

type progressFile struct {
	name      string
	started   time.Time
	completed time.Time
	sent      int64
	size      int64
	hash      string
	polled    int
}

func (f *progressFile) GetName() string {
	return f.name
}

func (f *progressFile) GetSize() int64 {
	return f.size
}

func (f *progressFile) GetHash() string {
	return f.hash
}

func (f *progressFile) GetStarted() time.Time {
	return f.started
}

func (f *progressFile) TimeMs() int64 {
	return int64(f.completed.Sub(f.started).Nanoseconds() / 1e6)
}
