package client

import (
	"fmt"
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
	StatPayload  bool             // Whether or not to record single-payload throughput stats
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
	statLock  sync.Mutex
	statSince time.Time
	bytesOut  int64

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
	defer log.Debug("Client done")
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
	if len(send) > 0 {
		go func(ch chan<- []sts.Hashed, files []sts.Hashed) {
			ch <- files
		}(broker.chScanned, send)
	}
	if len(poll) > 0 {
		for _, p := range poll {
			go func(ch chan<- sts.Pollable, f sts.Pollable) {
				ch <- f
			}(broker.chValidate, p)
		}
	}

	start := func(s func(wg *sync.WaitGroup), wg *sync.WaitGroup, n int) {
		wg.Add(n)
		for i := 0; i < n; i++ {
			go s(wg)
		}
	}

	var wgScanned,
		wgQueued,
		wgTransmit,
		wgTransmitted,
		wgValidate,
		wgValidated sync.WaitGroup

	start(broker.startScan, &wgScanned, 1)
	start(broker.startQueue, &wgQueued, 1)
	start(broker.startBin, &wgTransmit, 1)
	start(broker.startSend, &wgTransmitted, broker.Conf.Threads)
	start(broker.startTrack, &wgValidate, 1)
	start(broker.startValidate, &wgValidated, 1)

	broker.chStop <- <-stop
	close(broker.chStop)

	wgScanned.Wait()
	close(broker.chScanned)

	wgQueued.Wait()
	close(broker.chQueued)

	wgTransmit.Wait()
	close(broker.chTransmit)

	wgTransmitted.Wait()
	close(broker.chTransmitted)

	wgValidate.Wait()
	close(broker.chValidate)

	wgValidated.Wait()
	done <- true
}

func (broker *Broker) recover() (
	send []sts.Hashed, poll []sts.Pollable, err error) {
	var partials []*sts.Partial
	nErr := 0
	for {
		partials, err = broker.Conf.Recoverer()
		if err != nil {
			log.Error("Recovery request failed:", err.Error())
			nErr++
			// Wait longer the more it fails
			time.Sleep(time.Duration(nErr) * time.Second)
			continue
		}
		break
	}
	lookup := make(map[string]*sts.Partial)
	for _, p := range partials {
		lookup[p.Name] = p
		if broker.Conf.Cache.Get(p.Name) == nil {
			log.Error("Stale companion on target host:", p.Name)
		}
	}
	// TODO: handle old-style cache where MD5s weren't included?  If not, we
	// need to remove the cache file when migrating from a pre-1.0 release.
	store := broker.Conf.Store
	cache := broker.Conf.Cache
	cache.Iterate(func(f sts.Cached) bool {
		var file sts.File
		file, err = store.Sync(f)
		if store.IsNotExist(err) {
			if !f.IsDone() {
				// This should really only happen in rare scenarios when the
				// file is removed before the cache is updated (and later
				// persisted).
				log.Info("File disappeared:", f.GetPath())
			}
			cache.Remove(f.GetName())
			return false
		} else if err != nil {
			log.Error("Failed to stat cached file:", f.GetPath(), err.Error())
			return false
		} else if file != nil {
			log.Debug("Ignore changed file:", f.GetPath())
			return false
		}
		partial, exists := lookup[f.GetName()]
		if exists {
			log.Debug("Found partial:", f.GetName())
			// Partially sent; need to gracefully recover
			parts := make(chunks, len(partial.Parts))
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
			if len(missing) > 0 {
				send = append(send, &recovered{
					Cached: f,
					prev:   partial.Prev,
					left:   missing,
				})
				return false
			}
		}
		log.Debug("Found file to poll:", f.GetName())
		// Either fully sent or not at all; need to poll to find out which
		poll = append(poll, &progressFile{
			name:      f.GetName(),
			completed: time.Unix(f.GetTime(), 0),
			size:      f.GetSize(),
			hash:      f.GetHash(),
		})
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
					name:      cached.GetName(),
					completed: time.Unix(cached.GetTime(), 0),
					size:      cached.GetSize(),
					hash:      cached.GetHash(),
				})
				break
			case f.Failed():
				send = append(send, cached)
				break
			case f.Received():
				if !broker.Conf.Logger.WasSent(
					f.GetName(), time.Unix(cached.GetTime(), 0), time.Now()) {
					broker.Conf.Logger.Sent(&progressFile{
						name:      cached.GetName(),
						completed: time.Unix(cached.GetTime(), 0),
						size:      cached.GetSize(),
						hash:      cached.GetHash(),
					})
				}
				broker.finish(f)
				break
			}
		}
		// Make sure changes get persisted after the batch is processed
		if err = broker.Conf.Cache.Persist(time.Time{}); err != nil {
			log.Error(err.Error())
		}
	}
	err = nil
	return
}

func (broker *Broker) startScan(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Scanner done")
	for {
		log.Debug("Scanning ...")
		found := broker.scan()
		if len(found) > 0 {
			broker.chScanned <- found
		}
		log.Debug("Scan complete. Found:", len(found))
		select {
		case <-broker.chStop:
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
	age := cache.Boundary()

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
				tagName := broker.Conf.Tagger(file.GetName())
				if tag, ok := broker.tagMap[tagName]; ok && !tag.Delete {
					check = true
				}
			}
			// Skip files that aren't marked to be cleaned up and that have
			// mod times before the scan time minus the minimum age minus
			// the cache age
			if check && file.GetTime() < age.Unix() {
				log.Debug("Skipping due to age:", file.GetName())
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

	if err = broker.hash(wrapped,
		broker.Conf.Threads,
		int64(broker.Conf.PayloadSize)); err != nil {

		log.Error(err)
		return nil
	}

	// Update the cache
	cache.Iterate(func(cached sts.Cached) bool {
		if cached.IsDone() && cached.GetTime() < age.Unix() {
			cache.Remove(cached.GetName())
		}
		return false
	})
	for _, file := range wrapped {
		cache.Add(file)
	}
	if err = cache.Persist(
		scanTime.Add(-1 * broker.Conf.CacheAge)); err != nil {
		log.Error(err)
		return nil
	}

	return wrapped
}

func (broker *Broker) hash(
	scanned []sts.Hashed, nWorkers int, batchSize int64) error {

	hashFiles := func(in <-chan []sts.Hashed, wg *sync.WaitGroup) {
		defer wg.Done()
		for files := range in {
			log.Debug(fmt.Sprintf("HASHing %d files...", len(files)))
			var err error
			var fh sts.Readable
			for _, file := range files {
				log.Debug(fmt.Sprintf("HASHing %s ...", file.GetName()))
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
	}
	wg := sync.WaitGroup{}
	wg.Add(nWorkers)
	ch := make(chan []sts.Hashed, nWorkers*2)
	for i := 0; i < nWorkers; i++ {
		go hashFiles(ch, &wg)
	}
	j := 0
	count := 1
	var size int64
	for i, file := range scanned {
		size += file.GetSize()
		if size < batchSize {
			count++
			continue
		}
		ch <- scanned[j : j+count]
		j = i + 1
		count = 1
		size = 0
	}
	if j < len(scanned) {
		ch <- scanned[j:len(scanned)]
	}
	close(ch)
	wg.Wait()
	var n int
	for _, file := range scanned {
		if file.GetHash() == "" {
			n++
		}
	}
	if n > 0 {
		return fmt.Errorf("%d of %d files failed hash calculation",
			n, len(scanned))
	}
	return nil
}

func (broker *Broker) startQueue(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Sorter done")
	in := broker.chScanned
	out := broker.chQueued
	var wait <-chan time.Time
	var block bool
	for {
		if block {
			wait = nil
		} else {
			// Make the wait time very small to keep from there being a bottle
			// neck getting the file slices out of the Q
			wait = time.After(time.Millisecond * 1)
		}
		select {
		case batch, ok := <-in:
			if batch != nil {
				broker.Conf.Queue.Push(batch)
			}
			if !ok {
				in = nil
			}
			block = false
			break
		case <-wait:
			next := broker.Conf.Queue.Pop()
			if next == nil {
				if in == nil {
					// If the Q is empty and the input channel is closed, then
					// we're done
					return
				}
				block = true
				continue
			}
			out <- next
			break
		}
	}
}

func (broker *Broker) startBin(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Binner done")
	var payload sts.Payload
	var sendable sts.Sendable
	var current sts.Binnable
	var ok bool
	var timeout <-chan time.Time
	wait := time.Second * 1
	in := broker.chQueued
	out := broker.chTransmit
	for {
		if current == nil {
			if payload == nil {
				log.Debug("Bin blocking ...")
				// Just block if we don't have anything waiting
				timeout = nil
			} else {
				timeout = time.After(wait)
			}
			select {
			case sendable, ok = <-in:
				if sendable != nil {
					tagName := broker.Conf.Tagger(sendable.GetName())
					tag := broker.tagMap[tagName]
					current = &binnable{
						Sendable: sendable,
						tag:      tagName,
						noPrev:   tag == nil || tag.InOrder == false,
					}
				}
				if !ok {
					if payload != nil && payload.GetSize() > 0 {
						out <- payload
					}
					return
				}
			case <-timeout:
				log.Debug("Bin waited")
				if payload != nil && payload.GetSize() > 0 {
					out <- payload
					payload = nil
				}
				continue
			}
		}
		if payload == nil {
			payload = broker.Conf.BuildPayload(
				int64(broker.Conf.PayloadSize),
				broker.Conf.Store.GetOpener())
		}
		log.Debug("Payload:", current.GetName(), payload.IsFull())
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
	defer log.Debug("Sender done")
	in := broker.chTransmit
	out := broker.chTransmitted
	nErr := 0
	for {
		log.Debug("Send loop ...")
		payload, ok := <-in
		if !ok {
			return
		}
		for _, p := range payload.GetParts() {
			beg, end := p.GetSlice()
			log.Debug("Sending:", p.GetName(), beg, end)
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
				if f, err := broker.Conf.Store.Sync(
					file); f != nil || err != nil {
					// If the file changed, it will get picked up again
					// automatically, so we can just ignore it
					payload.Remove(binned)
				}
			}
			// Wait longer the more it fails
			time.Sleep(time.Duration(nErr) * time.Second)
		}
		broker.stat(payload)
		out <- payload
	}
}

func (broker *Broker) stat(payload sts.Payload) {
	if broker.Conf.StatPayload {
		s := payload.GetCompleted().Sub(payload.GetStarted()).Seconds()
		mb := float64(payload.GetSize()) / float64(1024) / float64(1024)
		log.Info(fmt.Sprintf(
			"Throughput: %3d part(s), %10.2f MB, %6.2f sec, %6.2f MB/sec",
			len(payload.GetParts()), mb, s, mb/s))
	}
	if broker.Conf.StatInterval == time.Duration(0) {
		return
	}
	broker.statLock.Lock()
	defer broker.statLock.Unlock()
	if broker.statSince.IsZero() {
		// Start the stats after the first payload
		broker.statSince = time.Now()
		return
	}
	broker.bytesOut += payload.GetSize()
	d := time.Now().Sub(broker.statSince)
	if d > broker.Conf.StatInterval {
		s := d.Seconds()
		mb := float64(broker.bytesOut) / float64(1024) / float64(1024)
		log.Info(fmt.Sprintf(
			"TOTAL Throughput: %.2fMB, %.2fs, %.2f MB/s", mb, s, mb/s))
		broker.bytesOut = 0
		broker.statSince = time.Now()
	}
}

func (broker *Broker) startTrack(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Tracker done")
	in := broker.chTransmitted
	out := broker.chValidate
	progress := make(map[string]*progressFile)
	var waiter sync.WaitGroup
	for {
		log.Debug("Track loop ...")
		payload, ok := <-in
		if !ok {
			waiter.Wait()
			return
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
			offset, n := binned.GetSlice()
			pFile.sent += n
			if pFile.sent >= pFile.size {
				if offset == 0 && n == binned.GetFileSize() {
					// Compute the "completed" time based on how long the
					// proportion of this complete file's size to that of the
					// bin's.  This is to get a more accurate amount of time
					// it took to send the file.
					t := payload.GetCompleted().Sub(
						payload.GetStarted()).Nanoseconds()
					r := float64(n) / float64(payload.GetSize())
					d := time.Duration(float64(t) * r)
					pFile.completed = pFile.started.Add(d)
				} else {
					pFile.completed = payload.GetCompleted()
				}
				broker.Conf.Logger.Sent(pFile)
				delete(progress, key)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					wg.Add(1)
					out <- pFile
				}(&waiter)
			}
		}
	}
}

func (broker *Broker) startValidate(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Validator done")
	in := broker.chValidate
	poll := make(map[string]*progressFile)
	wait := broker.Conf.PollDelay
	var timeout <-chan time.Time
	var pollTime time.Time
	var sent sts.Pollable
	var ready []sts.Pollable
	var polled []sts.Polled
	var ok bool
	var err error
	var nErr int
	for {
		if len(poll) == 0 {
			if in == nil {
				// We can safely return once our Q is empty
				return
			}
			// If there's nothing in our Q, just block until we get a file
			timeout = nil
		} else {
			timeout = time.After(wait)
		}
		select {
		case sent, ok = <-in:
			if !ok {
				// Receiving on a nil channel will block, which is what we want
				// since the timeout will keep from thrashing while we work on
				// remaining files to be polled
				in = nil
			}
			break
		case <-timeout:
			break
		}
		if sent != nil {
			poll[sent.GetName()] = sent.(*progressFile)
			sent = nil
		}
		// Make sure it's been long enough to send another poll
		if !pollTime.IsZero() &&
			time.Now().Sub(pollTime) < broker.Conf.PollInterval {
			continue
		}
		// Build the list of pollable files
		ready = nil
		for _, f := range poll {
			if time.Now().Sub(f.completed) >= broker.Conf.PollDelay {
				log.Debug("Polling:", f.GetName())
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
		// Update the poll time only after the request has completed so as to
		// not include the request time in the interval delay
		pollTime = time.Now()
		for _, f := range polled {
			switch {
			case f.NotFound():
				poll[f.GetName()].polled++
				if poll[f.GetName()].polled == broker.Conf.PollAttempts {
					log.Error("Exceeded maximum polling attempts:", f.GetName())
					broker.finish(f)
					delete(poll, f.GetName())
				}
				break
			case f.Waiting():
				// Nothing to do with "waiting" files. They should continue to
				// be polled since eventually they will be released on the
				// receiving end
				break
			case f.Failed():
				log.Error("Failed validation:", f.GetName())
				fallthrough
			case f.Received():
				broker.finish(f)
				delete(poll, f.GetName())
				break
			}
		}
		// Make sure changes get persisted after the batch is processed
		if err = broker.Conf.Cache.Persist(time.Time{}); err != nil {
			log.Error(err.Error())
		}
	}
}

func (broker *Broker) finish(file sts.Polled) {
	switch {
	case file.Received():
		log.Debug("Received:", file.GetName())
		cached := broker.Conf.Cache.Get(file.GetName())
		tagName := broker.Conf.Tagger(file.GetName())
		tag := broker.tagMap[tagName]
		if tag != nil && tag.Delete {
			broker.Conf.Store.Remove(cached)
		}
		fallthrough
	default:
		broker.Conf.Cache.Done(file.GetName())
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

func (f *hashFile) GetName() string {
	return f.orig.GetName()
}

func (f *hashFile) GetSize() int64 {
	return f.orig.GetSize()
}

func (f *hashFile) GetTime() int64 {
	return f.orig.GetTime()
}

func (f *hashFile) GetMeta() []byte {
	return f.orig.GetMeta()
}

func (f *hashFile) GetHash() string {
	return f.hash
}

type recovered struct {
	sts.Cached
	prev string
	left []*sts.ByteRange
	part int
	used int64
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
	sts.Sendable
	tag       string
	noPrev    bool
	allocated int64
}

func (f *binnable) GetPrev() string {
	if f.noPrev {
		return ""
	}
	return f.Sendable.GetPrev()
}

func (f *binnable) GetNextAlloc() (int64, int64) {
	b, n := f.GetSlice()
	return b + f.allocated, b + n
}

func (f *binnable) AddAlloc(n int64) {
	f.allocated += n
}

func (f *binnable) IsAllocated() bool {
	_, n := f.GetSlice()
	return f.allocated == n
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
