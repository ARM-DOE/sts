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

const (
	maxPoll = 1000
)

// Conf is the struct for storing all the settable components and options for
// this client to manage the outgoing data flow
type Conf struct {
	Name string // The name assigned to this broker/client instance

	// Components
	Store        sts.FileSource          // The source of file objects from which to read (and delete)
	Cache        sts.FileCache           // The persistent cache of file metadata from the store
	Queue        sts.FileQueue           // The queue that takes files ready to send and chunks them up in its configured order
	Recoverer    sts.Recover             // The function that determines what chunks need to be sent first
	BuildPayload sts.PayloadFactory      // The function that creates a Payload instance
	Transmitter  sts.Transmit            // The transmission implementation
	TxRecoverer  sts.RecoverTransmission // The transmission recovery implementation
	Validator    sts.Validate            // The validation implementation
	Logger       sts.SendLogger          // The logger used to log completed files

	// Helpers
	Tagger  sts.Translate // Takes a file name and provides a tag name
	Renamer sts.Rename    // Takes a file and provides a new target file name

	// Options
	CacheAge     time.Duration    // How long to keep files in the cache after being sent
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
	Name        string // The name of the tag to be matched by the tagger
	InOrder     bool   // Whether or not the order files are received matters
	Delete      bool
	DeleteDelay time.Duration
}

// Broker is the client struct
type Broker struct {
	Conf         *Conf
	tagMap       map[string]*FileTag
	cleanAll     bool
	cleanSome    bool
	stuckSince   time.Time
	throughput   *throughputMonitor
	stop         bool
	stopMux      sync.RWMutex
	stopGraceful bool

	// Channels
	chStop        chan bool
	chScanned     chan []sts.Hashed
	chQueued      chan sts.Sendable
	chRetry       chan sts.Polled
	chTransmit    chan sts.Payload
	chTransmitted chan sts.Payload
	chStats       chan sts.Payload
	chValidate    chan sts.Pollable
}

// Start runs the client
func (broker *Broker) Start(stop <-chan bool, done chan<- bool) {
	defer log.Debug("Client done")
	defer func() {
		done <- true
	}()
	broker.throughput = &throughputMonitor{
		logInterval: broker.Conf.StatInterval,
	}
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
	broker.chRetry = make(chan sts.Polled, broker.Conf.Threads*2)
	broker.chTransmit = make(chan sts.Payload, broker.Conf.Threads*2)
	broker.chTransmitted = make(chan sts.Payload, broker.Conf.Threads*2)
	broker.chStats = make(chan sts.Payload, broker.Conf.Threads*2)
	broker.chValidate = make(chan sts.Pollable, broker.Conf.Threads*2)

	stopNow := make(chan bool)

	go func() {
		// Wait for an internal or external stop signal
		var stopGraceful bool
		select {
		case stopGraceful = <-stop:
		case <-stopNow:
		}
		// Start stopping ...
		broker.stopMux.Lock()
		broker.stop = true
		broker.stopGraceful = stopGraceful
		broker.stopMux.Unlock()
		broker.chStop <- stopGraceful
		close(broker.chStop)
	}()

	send, err := broker.recover()
	if broker.shouldStopNow() {
		return
	}
	if err != nil {
		broker.error("Recovery failed:", err.Error())
	} else if len(send) > 0 {
		// This shouldn't block since we have a buffer of 1 in this channel
		broker.chScanned <- send
	}

	start := func(s func(wg *sync.WaitGroup), wg *sync.WaitGroup, n int) {
		wg.Add(n)
		for i := 0; i < n; i++ {
			go s(wg)
		}
	}

	var wgScanned,
		wgQueued,
		wgFailed,
		wgTransmit,
		wgTransmitted,
		wgStats,
		wgValidate,
		wgValidated sync.WaitGroup

	start(broker.startScan, &wgScanned, 1)
	start(broker.startQueue, &wgQueued, 1)
	start(broker.startRetry, &wgFailed, broker.Conf.Threads)
	start(broker.startBin, &wgTransmit, 1)
	start(broker.startSend, &wgTransmitted, broker.Conf.Threads)
	start(broker.startStats, &wgStats, 1)
	start(broker.startTrack, &wgValidate, 1)
	start(broker.startValidate, &wgValidated, 1)

	broker.info("STARTED")

	wgScanned.Wait()
	// Need to wait until this is done also before closing a channel that
	// the retrier may write to
	wgFailed.Wait()
	close(broker.chScanned)

	wgQueued.Wait()
	close(broker.chQueued)

	wgTransmit.Wait()
	close(broker.chTransmit)

	wgTransmitted.Wait()
	close(broker.chTransmitted)

	wgValidate.Wait()
	close(broker.chValidate)
	close(broker.chStats)
	wgStats.Wait()

	wgValidated.Wait()
	close(broker.chRetry)

	wgFailed.Wait()
}

func (broker *Broker) info(params ...interface{}) {
	log.Info(append([]interface{}{"(" + broker.Conf.Name + ")"}, params...)...)
}

func (broker *Broker) error(params ...interface{}) {
	log.Error(append([]interface{}{"(" + broker.Conf.Name + ")"}, params...)...)
}

func (broker *Broker) shouldStopNow() bool {
	broker.stopMux.RLock()
	defer broker.stopMux.RUnlock()
	return broker.stop && !broker.stopGraceful
}

// GetState returns the standard state struct providing a snapshot of the client
func (broker *Broker) GetState() sts.SourceState {
	state := sts.SourceState{}
	var oldestFound sts.Cached
	var newestFound sts.Cached
	var oldestQueued sts.Cached
	var newestQueued sts.Cached
	var oldestComplete sts.Cached
	var newestComplete sts.Cached
	broker.Conf.Cache.Iterate(func(f sts.Cached) bool {
		switch {
		case f.GetHash() == "":
			state.FoundCount++
			state.FoundSize += f.GetSize()
			if oldestFound == nil || f.GetTime().Before(oldestFound.GetTime()) {
				oldestFound = f
			}
			if newestFound == nil || f.GetTime().After(newestFound.GetTime()) {
				newestFound = f
			}
		case !f.IsDone():
			state.QueueCount++
			state.QueueSize += f.GetSize()
			if oldestQueued == nil || f.GetTime().Before(oldestQueued.GetTime()) {
				oldestQueued = f
			}
			if newestQueued == nil || f.GetTime().After(newestQueued.GetTime()) {
				newestQueued = f
			}
		default:
			state.SentCount++
			state.SentSize += f.GetSize()
			if oldestComplete == nil || f.GetTime().Before(oldestComplete.GetTime()) {
				oldestComplete = f
			}
			if newestComplete == nil || f.GetTime().After(newestComplete.GetTime()) {
				newestComplete = f
			}
		}
		return false
	})
	bytes, seconds, pctIdle := broker.throughput.compute()
	state.Throughput = sts.Throughput{
		Bytes: bytes, Seconds: seconds, PctIdle: pctIdle,
	}
	cacheFiles := []sts.Cached{
		oldestFound, newestFound,
		oldestQueued, newestQueued,
		oldestComplete, newestComplete,
	}
	state.RecentFiles = make([]*sts.RecentFile, len(cacheFiles))
	for i, cacheFile := range cacheFiles {
		if cacheFile == nil {
			state.RecentFiles[i] = nil
			continue
		}
		renamed := ""
		if broker.Conf.Renamer != nil {
			renamed = broker.Conf.Renamer(cacheFile)
		}
		state.RecentFiles[i] = &sts.RecentFile{
			Name:   cacheFile.GetName(),
			Rename: renamed,
			Path:   cacheFile.GetPath(),
			Size:   cacheFile.GetSize(),
			Hash:   cacheFile.GetHash(),
			Time:   cacheFile.GetTime(),
			Sent:   cacheFile.IsDone(),
		}
	}
	return state
}

func (broker *Broker) recover() (send []sts.Hashed, err error) {
	var poll []sts.Pollable
	var partials []*sts.Partial
	nErr := 0
	for {
		if broker.shouldStopNow() {
			return
		}
		broker.info("STARTUP: Requesting recovery information from server ...")
		partials, err = broker.Conf.Recoverer()
		if err != nil {
			broker.error("Recovery request failed:", err.Error())
			nErr++
			// Wait longer the more it fails
			time.Sleep(time.Duration(nErr) * time.Second)
			continue
		}
		break
	}
	broker.info(fmt.Sprintf(
		"STARTUP: Found %d files to recover",
		len(partials),
	))
	store := broker.Conf.Store
	cache := broker.Conf.Cache
	lookup := make(map[string]*sts.Partial)
	for _, p := range partials {
		if broker.shouldStopNow() {
			return
		}
		cached := cache.Get(p.Name)
		if cached == nil {
			// We're putting it in the send Q only to get the right order
			send = append(send, &recoverFile{
				Cached: &placeholderFile{
					name: p.Name,
					size: p.Size,
					time: p.Time.Time,
					hash: p.Hash,
				},
			})
			continue
		}
		if cached.IsDone() {
			// We're putting it in the send Q only to get the right order
			send = append(send, &recoverFile{
				Cached: cached,
			})
			continue
		}
		lookup[p.Name] = p
	}
	cache.Iterate(func(f sts.Cached) bool {
		if broker.shouldStopNow() {
			return true
		}
		if f.IsDone() {
			return false
		}
		var file sts.File
		file, err = store.Sync(f)
		if store.IsNotExist(err) {
			cache.Done(f.GetName(), nil)
			return false
		} else if err != nil {
			broker.error("Failed to stat cached file:", f.GetPath(), err.Error())
			return false
		} else if file != nil {
			log.Debug("Ignore changed file:", f.GetPath())
			return false
		} else if f.GetHash() == "" {
			log.Debug("Ignore file without a hash:", f.GetPath())
			return false
		}
		if partial, ok := lookup[f.GetName()]; ok {
			log.Debug("Found partial:", f.GetName())
			// Partially sent; need to gracefully recover
			parts := make(chunks, len(partial.Parts))
			copy(parts, partial.Parts)
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
				send = append(send, &recoverFile{
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
			started:   f.GetTime(),
			completed: f.GetTime(),
			size:      f.GetSize(),
			hash:      f.GetHash(),
		})
		return false
	})
	for {
		if broker.shouldStopNow() {
			return
		}
		nPoll := len(poll)
		if nPoll == 0 {
			break
		}
		pollNow := poll
		if nPoll > maxPoll {
			pollNow = poll[:maxPoll]
			poll = poll[maxPoll:]
		} else {
			poll = nil
		}
		broker.info(fmt.Sprintf(
			"STARTUP: Asking server if %d (of %d) files were fully sent ...",
			len(pollNow),
			nPoll,
		))
		var polled []sts.Polled
		if polled, err = broker.Conf.Validator(pollNow); err != nil {
			return
		}
		broker.info("STARTUP: Processing server response ...")
		for _, f := range polled {
			if broker.shouldStopNow() {
				return
			}
			cached := cache.Get(f.GetName())
			switch {
			case f.NotFound():
				send = append(send, cached)
			case f.Failed():
				log.Debug("Recovering previous failure:", f.GetName())
				if partial, ok := lookup[f.GetName()]; ok {
					send = append(send, &recoverFile{
						Cached: cached,
						prev:   partial.Prev,
						left: chunks{
							&sts.ByteRange{
								Beg: 0,
								End: cached.GetSize(),
							},
						},
					})
				} else {
					send = append(send, cached)
				}
			case f.Waiting() || f.Received():
				if !broker.Conf.Logger.WasSent(
					f.GetName(), f.GetHash(), cached.GetTime(), time.Now(),
				) {
					broker.Conf.Logger.Sent(&progressFile{
						name:      cached.GetName(),
						started:   cached.GetTime(),
						completed: cached.GetTime(),
						size:      cached.GetSize(),
						hash:      cached.GetHash(),
					})
				}
				// We're putting it in the send Q only to get the right order
				send = append(send, &recoverFile{
					Cached: cached,
				})
				broker.finish(f)
			}
		}
		// Make sure changes get persisted after the batch is processed
		if err = broker.Conf.Cache.Persist(); err != nil {
			broker.error(err.Error())
		}
	}
	broker.info(fmt.Sprintf("STARTUP: Recovery done (%d files to be recovered)", len(send)))
	err = nil
	return
}

func (broker *Broker) startScan(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Scanner done")
	var wait <-chan time.Time
	for {
		log.Debug("Scanning ...")
		found := broker.scan()
		if len(found) > 0 {
			// broker.chScanned <- found
			if !sendCh(broker.shouldStopNow, broker.chScanned, found, 0) {
				return
			}
		}
		log.Debug("Scan complete. Found:", len(found))
		wait = time.After(broker.Conf.ScanDelay)
		select {
		case <-broker.chStop:
			return
		case <-wait:
		}
	}
}

func (broker *Broker) includeScannedFile(file sts.File) bool {
	if file.GetSize() == 0 {
		log.Debug("Skipping zero-length file:", file.GetName())
		return false
	}
	cached := broker.Conf.Cache.Get(file.GetName())
	if cached != nil {
		// A file in the cache should be ignored unless it has changed
		changed := cached.GetSize() != file.GetSize() ||
			cached.GetTime() != file.GetTime()
		if changed {
			log.Debug("Detected changed file:", file.GetName())
		}
		return changed
	}
	return true
}

func (broker *Broker) getTag(file sts.File) *FileTag {
	tagName := broker.Conf.Tagger(file.GetName())
	if tag, ok := broker.tagMap[tagName]; ok {
		return tag
	}
	return nil
}

func (broker *Broker) canDelete(file sts.File) bool {
	if !broker.cleanSome {
		return false
	}
	if tag := broker.getTag(file); tag != nil {
		if tag.Delete {
			if tag.DeleteDelay == 0 {
				return true
			}
			return time.Since(file.GetTime()) > tag.DeleteDelay
		}
	}
	return false
}

func (broker *Broker) scan() []sts.Hashed {
	var err error
	var files []sts.File

	store := broker.Conf.Store
	cache := broker.Conf.Cache

	if broker.stuckSince.IsZero() {
		broker.stuckSince = time.Now()
	} else if time.Since(broker.stuckSince) > broker.Conf.CacheAge {
		// Every cache age interval, check the files that have been in the
		// cache at least that long to see if they still exist.  Remove the
		// ones that don't and simply log the ones that do and aren't "done"
		// yet.
		//
		// This helps with pesky files like .nfs000* that come and go from the
		// file system and need to not linger in our cache.  It also helps in
		// the case where files are NOT cleaned up by STS but are cleaned up
		// outside of STS.
		cache.Iterate(func(cached sts.Cached) bool {
			if broker.shouldStopNow() {
				return true
			}
			if cached.GetTime().Before(broker.stuckSince) {
				if _, err := store.Sync(cached); store.IsNotExist(err) {
					broker.info("Not found--removing from cache:", cached.GetName())
					cache.Remove(cached.GetName())
					return false
				}
				if !cached.IsDone() {
					broker.info("Potentially stuck file:", cached.GetName())
				}
			}
			return false
		})
		if broker.shouldStopNow() {
			return nil
		}
		broker.stuckSince = time.Now()
	}

	// Perform the scan
	if files, _, err = store.Scan(broker.includeScannedFile); err != nil {
		broker.error(err)
		return nil
	}

	if broker.shouldStopNow() {
		return nil
	}

	// Wrap the scanned file objects in type that can have hash added
	wrapped := make([]sts.Hashed, len(files))
	for i, file := range files {
		wrapped[i] = &hashFile{File: file}
		if broker.Conf.Renamer != nil {
			log.Debug("Rename:", file.GetName(), "->", broker.Conf.Renamer(file))
		}
	}

	// Add any stragglers and clean the cache (and store, if applicable)
	cache.Iterate(func(cached sts.Cached) bool {
		if broker.shouldStopNow() {
			return true
		}
		switch {
		case cached.GetHash() == "":
			// Add any that might have failed the hash calculation last time
			wrapped = append(wrapped, &hashFile{File: cached})
		case cached.IsDone() && broker.canDelete(cached):
			err = broker.Conf.Store.Remove(cached)
			if err != nil {
				broker.error("Failed to delete aged file:", cached.GetName())
				break
			}
			broker.info("Deleted aged file:", cached.GetName())
			cache.Remove(cached.GetName())
			log.Debug("Removed from cache:", cached.GetName())
		}
		return false
	})

	if broker.shouldStopNow() {
		return nil
	}

	// Update the cache
	var ready []sts.Hashed
	if len(wrapped) > 0 {
		var n int
		if n, err = broker.hash(
			wrapped,
			broker.Conf.Threads,
			int64(broker.Conf.PayloadSize),
		); err != nil {
			broker.error(err)
		}
		if broker.shouldStopNow() {
			return nil
		}
		if n == len(wrapped) {
			ready = wrapped
		}
		for _, file := range wrapped {
			if broker.shouldStopNow() {
				return nil
			}
			// Update the cache with all files--even those without a hash (will
			// try again next time)--unless the file is missing
			if file.GetHash() == "" {
				if _, err = store.Sync(file); store.IsNotExist(err) {
					broker.info("Removed missing file from cache:", file.GetName())
					cache.Remove(file.GetName())
					continue
				}
			}
			cache.Add(file)
			if n < len(wrapped) && file.GetHash() != "" {
				ready = append(ready, file)
			}
		}
	}
	if err = cache.Persist(); err != nil {
		broker.error(err)
		return nil
	}
	return ready
}

func (broker *Broker) hashFiles(opener sts.Open, in <-chan []sts.Hashed, wg *sync.WaitGroup) {
	defer wg.Done()
	for files := range in {
		log.Debug(fmt.Sprintf("HASHing %d files...", len(files)))
		var err error
		var fh sts.Readable
		for _, file := range files {
			if broker.shouldStopNow() {
				break
			}
			log.Debug(fmt.Sprintf("HASHing %s ...", file.GetName()))
			fh, err = opener(file)
			if err != nil {
				broker.error("Failed to generate hash:", err)
				continue
			}
			file.(*hashFile).hash, err = fileutil.ReadableMD5(fh)
			if err != nil {
				broker.error(err)
			}
			fh.Close()
			log.Debug(fmt.Sprintf("HASHed %s : %s", file.GetName(), file.(*hashFile).hash))
		}
	}
}

func (broker *Broker) hash(
	scanned []sts.Hashed,
	nWorkers int,
	batchSize int64,
) (int, error) {
	wg := sync.WaitGroup{}
	wg.Add(nWorkers)
	ch := make(chan []sts.Hashed, nWorkers*2)
	opener := broker.Conf.Store.GetOpener()
	for i := 0; i < nWorkers; i++ {
		go broker.hashFiles(opener, ch, &wg)
	}
	j := 0
	count := 1
	var size int64
	for i, file := range scanned {
		if broker.shouldStopNow() {
			// Closing the channel to stop the running go routines
			close(ch)
			return 0, nil
		}
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
		ch <- scanned[j:]
	}
	close(ch)
	wg.Wait()
	n := 0
	for _, file := range scanned {
		if file.GetHash() != "" {
			n++
		}
	}
	return n, nil
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
				// OK to stop before the Q is empty if not doing it gracefully
				if broker.shouldStopNow() {
					return
				}
				in = nil
			}
			block = false
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
			// out <- next
			if !sendCh(broker.shouldStopNow, out, next, 0) {
				return
			}
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
	var wait <-chan time.Time
	in := broker.chQueued
	out := broker.chTransmit
	for {
		if current == nil {
			if payload == nil {
				log.Debug("Bin blocking ...")
				// Just block if we don't have anything waiting
				wait = nil
			} else {
				wait = time.After(time.Second * 1)
			}
			select {
			case sendable, ok = <-in:
				if broker.shouldStopNow() {
					return
				}
				if sendable != nil {
					tagName := broker.Conf.Tagger(sendable.GetName())
					tag := broker.tagMap[tagName]
					current = &binnable{
						Sendable: sendable,
						tag:      tagName,
						noPrev:   tag == nil || !tag.InOrder,
					}
				}
				if !ok {
					if payload != nil && payload.GetSize() > 0 {
						// out <- payload
						if !sendCh(broker.shouldStopNow, out, payload, 0) {
							return
						}
					}
					return
				}
			case <-wait:
				if broker.shouldStopNow() {
					return
				}
				log.Debug("Bin waited")
				if payload != nil && payload.GetSize() > 0 {
					// out <- payload
					if !sendCh(broker.shouldStopNow, out, payload, 0) {
						return
					}
					payload = nil
				}
				continue
			}
		}
		if payload == nil {
			payload = broker.Conf.BuildPayload(
				int64(broker.Conf.PayloadSize),
				broker.Conf.Store.GetOpener(),
				broker.Conf.Renamer)
		}
		log.Debug("Payload:", current.GetName(), payload.IsFull())
		added := payload.Add(current)
		if !added || current.IsAllocated() {
			current = nil
		}
		if payload.IsFull() {
			// out <- payload
			if !sendCh(broker.shouldStopNow, out, payload, 0) {
				return
			}
			payload = nil
		}
	}
}

func (broker *Broker) startSend(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Sender done")
	in := broker.chTransmit
	out := broker.chTransmitted
	var nErr int
	for {
		log.Debug("Send loop ...")
		payload, ok := <-in
		if !ok || broker.shouldStopNow() {
			return
		}
		for _, p := range payload.GetParts() {
			beg, len := p.GetSlice()
			log.Debug("Sending:", p.GetName(), beg, beg+len,
				"T:", p.GetFileTime())
		}
		nErr = 0
		for {
			if broker.shouldStopNow() {
				return
			}
			n, err := broker.Conf.Transmitter(payload)
			if err == nil {
				break
			}
			nErr++
			broker.error("Payload send failed:", err.Error())
			payload = broker.handleSendError(payload, n)
			if payload == nil || len(payload.GetParts()) == 0 {
				// It's entirely possible that the problem occurred after all
				// parts were received by the server and we can simply move on
				payload = nil
				break
			}
			// Check each file in the payload to make sure none of them
			// has changed and remove the ones that have
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
					broker.info("Ignoring changed file in payload:", binned.GetName())
					payload.Remove(binned)
				}
			}
			if len(payload.GetParts()) == 0 {
				log.Debug("Skipping now empty payload")
				payload = nil
				break
			}
			// Wait longer the more it fails
			time.Sleep(time.Duration(nErr) * time.Second)
		}
		if payload != nil {
			broker.stat(payload)
			// out <- payload
			if !sendCh(broker.shouldStopNow, out, payload, 0) {
				break
			}
		}
	}
}

func sendCh[T any](
	shouldStop func() bool, ch chan T, item T, grace time.Duration,
) bool {
	if grace == 0 {
		grace = time.Second * 1
	}
	timer := time.NewTimer(grace)
	for {
		select {
		case ch <- item:
			if !timer.Stop() {
				<-timer.C
			}
			return true
		case <-timer.C:
			if shouldStop() {
				return false
			}
			timer.Reset(grace)
		}
	}
}

func recvCh[T any](
	shouldStop func() bool, ch chan T, grace time.Duration,
) *T {
	if grace == 0 {
		grace = time.Second * 1
	}
	timer := time.NewTimer(grace)
	for {
		select {
		case item := <-ch:
			if !timer.Stop() {
				<-timer.C
			}
			return &item
		case <-timer.C:
			if shouldStop() {
				return nil
			}
			timer.Reset(grace)
		}
	}
}

func (broker *Broker) handleSendError(payload sts.Payload, nPartsReceived int) sts.Payload {
	nErr := 0
	var n int
	var err error
	for {
		if broker.shouldStopNow() {
			break
		}
		if nPartsReceived == 0 {
			// If the server didn't respond with a number of parts received, we
			// need to make that request explicitly
			n, err = broker.Conf.TxRecoverer(payload)
		}
		if err == nil {
			broker.info("Transmission recovery:", n, "of", len(payload.GetParts()))
			if n > 0 {
				next := payload.Split(n)
				broker.stat(payload)
				// broker.chTransmitted <- payload
				if !sendCh(
					broker.shouldStopNow,
					broker.chTransmitted,
					payload,
					0,
				) {
					break
				}
				payload = next
			}
			break
		}
		nErr++
		broker.error("Payload recovery request failed:", err.Error())
		// Wait longer the more it fails
		time.Sleep(time.Duration(nErr) * time.Second)
	}
	return payload
}

func (broker *Broker) stat(payload sts.Payload) {
	if broker.Conf.StatPayload {
		s := payload.GetCompleted().Sub(payload.GetStarted()).Seconds()
		mb := float64(payload.GetSize()) / float64(1024) / float64(1024)
		broker.info(fmt.Sprintf(
			"Throughput: %3d part(s), %10.2f MB, %6.2f sec, %6.2f MB/sec",
			len(payload.GetParts()), mb, s, mb/s))
	}
	broker.chStats <- payload
}

func (broker *Broker) startStats(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Stats done")
	for payload := range broker.chStats {
		broker.throughput.add(
			payload.GetSize(), payload.GetStarted(), payload.GetCompleted(),
		)
	}
}

func (broker *Broker) startTrack(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Tracker done")
	in := broker.chTransmitted
	out := broker.chValidate
	progress := make(map[string]*progressFile)
	var wait <-chan time.Time
	var payload sts.Payload
	var ok bool
	for {
		if broker.shouldStopNow() {
			return
		}
		log.Debug("Track loop ...")
		// Block by default
		wait = nil
		if len(progress) == 0 {
			if in == nil {
				// We can safely return now that our Q is empty
				return
			}
		} else {
			// Send anything from the Q that's ready, but don't block if the
			// outgoing channel is full
			for key, pFile := range progress {
				if pFile.sent < pFile.size {
					continue
				}
				wait = time.After(time.Millisecond)
				select {
				case out <- pFile:
					delete(progress, key)
				case <-wait:
				}
			}
			wait = nil
			if len(progress) > 0 {
				// If the Q is still not empty, don't block when looking for a
				// new payload to receive
				wait = time.After(time.Second)
			}
		}
		select {
		case payload, ok = <-in:
			if !ok {
				// Receiving on a nil channel will block, which is what we want
				// since the timeout will keep from thrashing while we work on
				// remaining files to be polled
				in = nil
			}
		case <-wait:
		}
		if payload == nil {
			continue
		}
		for _, binned := range payload.GetParts() {
			key := binned.GetName()
			pFile, ok := progress[key]
			if !ok {
				progress[key] = &progressFile{
					name:    binned.GetName(),
					size:    binned.GetSendSize(),
					started: payload.GetStarted(),
					hash:    binned.GetFileHash(),
				}
				pFile = progress[key]
			} else if pFile.hash != binned.GetFileHash() {
				pFile.sent = 0
				pFile.size = binned.GetSendSize()
				pFile.started = payload.GetStarted()
				pFile.hash = binned.GetFileHash()
			}
			pFile.prev = binned.GetPrev()
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
			}
		}
	}
}

func (broker *Broker) startValidate(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Validator done")
	in := broker.chValidate
	poll := make(map[string]*progressFile)
	var wait <-chan time.Time
	var pollTime time.Time
	var sent sts.Pollable
	var ready []sts.Pollable
	var polled []sts.Polled
	var ok bool
	var err error
	var nErr int
	for {
		if broker.shouldStopNow() {
			return
		}
		if len(poll) == 0 {
			if in == nil {
				// We can safely return once our Q is empty
				return
			}
			// If there's nothing in our Q, just block until we get a file
			wait = nil
		} else {
			wait = time.After(broker.Conf.PollDelay)
		}
		select {
		case sent, ok = <-in:
			if !ok {
				// Receiving on a nil channel will block, which is what we want
				// since the timeout will keep from thrashing while we work on
				// remaining files to be polled
				in = nil
			}
		case <-wait:
		}
		if sent != nil {
			poll[sent.GetName()] = sent.(*progressFile)
			sent = nil
		}
		// Make sure it's been long enough to send another poll
		if !pollTime.IsZero() &&
			time.Since(pollTime) < broker.Conf.PollInterval {
			continue
		}
		// Build the list of pollable files

		ready = nil
		for _, f := range poll {
			if time.Since(f.completed) >= broker.Conf.PollDelay {
				log.Debug("Polling:", f.GetName())
				ready = append(ready, f)
				// Let's limit the number of files polled in a single request
				if len(ready) == maxPoll {
					break
				}
			}
		}
		log.Debug("Polling Status:", len(ready), "<-", len(poll), "(Backlog)")
		if len(ready) == 0 {
			// No files ready yet
			continue
		}
		nErr = 0
		for {
			if broker.shouldStopNow() {
				return
			}
			polled, err = broker.Conf.Validator(ready)
			if err != nil {
				broker.error("Poll request failed:", err.Error())
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
					broker.error("Exceeded maximum polling attempts:", f.GetName())
					broker.finish(f)
					delete(poll, f.GetName())
				}
			case f.Failed():
				broker.error("Failed validation:", f.GetName())
				fallthrough
			case f.Waiting():
				// Files "waiting" have been validated and can be cleaned up
				fallthrough
			case f.Received():
				broker.finish(f)
				delete(poll, f.GetName())
			}
		}
		// Make sure changes get persisted after the batch is processed
		if err = broker.Conf.Cache.Persist(); err != nil {
			broker.error(err.Error())
		}
	}
}

func (broker *Broker) finish(file sts.Polled) {
	switch {
	case file.Waiting() || file.Received():
		log.Debug("Validated:", file.GetName())
		// Make marking done and file removal a single transaction so that we
		// keep the cache in sync with the file system.  Without it, it's
		// possible (but not likely) that the cache could be written with a
		// file in the "done" state but wasn't cleaned up, which means it
		// would eventually be removed from the cache (age off) and then get
		// picked up again to be sent redundantly.
		broker.Conf.Cache.Done(file.GetName(), func(cached sts.Cached) {
			if broker.canDelete(cached) {
				broker.Conf.Store.Remove(cached)
				log.Debug("Deleted:", cached.GetName())
			}
		})
	default:
		log.Debug("Trying again:", file.GetName())
		// broker.chRetry <- file
		sendCh(broker.shouldStopNow, broker.chRetry, file, 0)
	}
}

func (broker *Broker) startRetry(wg *sync.WaitGroup) {
	defer wg.Done()
	defer log.Debug("Retry-er done")
	store := broker.Conf.Store
	cache := broker.Conf.Cache
	opener := store.GetOpener()
	var err error
	var hashed *hashFile
	var cached sts.Cached
	var fh sts.Readable
	var recovered []sts.Hashed
	var filePtr *sts.Polled
	var file sts.Polled
	for {
		filePtr = recvCh(broker.shouldStopNow, broker.chRetry, 0)
		if filePtr == nil {
			return
		}
		file = *filePtr
		cached = cache.Get(file.GetName())
		if cached == nil {
			// Maybe redundant attempts to send the same file occurred and one
			// of them failed while the other since succeeded and was removed
			// from the cache?
			broker.info("Ignoring missing failed file:", file.GetName())
			continue
		}
		if f, err := broker.Conf.Store.Sync(cached); f != nil || err != nil {
			// OK to ignore cases like this since changed files are picked up
			// automatically and put in the send Q again
			broker.info("Ignoring changed failed file:", file.GetName())
			continue
		}
		hashed = &hashFile{File: cached}
		log.Debug("Recomputing hash:", file.GetName())
		fh, err = opener(hashed)
		if err != nil {
			broker.error(err)
			if store.IsNotExist(err) {
				cache.Done(file.GetName(), nil)
			}
			continue
		}
		hashed.hash, err = fileutil.ReadableMD5(fh)
		if err != nil {
			broker.error(err)
		}
		fh.Close()
		cache.Add(hashed)
		log.Debug("Recomputed hash:", file.GetName(), hashed.hash)
		cached = cache.Get(file.GetName())
		// Use a "recovered" file in order to keep the "prev" intact
		recovered = []sts.Hashed{&recoverFile{
			Cached: cached,
			prev:   file.GetPrev(),
			left:   []*sts.ByteRange{{Beg: 0, End: cached.GetSize()}},
		}}
		if !sendCh(broker.shouldStopNow, broker.chScanned, recovered, 0) {
			return
		}
	}
}

type placeholderFile struct {
	name string
	size int64
	time time.Time
	hash string
}

func (f *placeholderFile) GetPath() string {
	return f.name
}

func (f *placeholderFile) GetName() string {
	return f.name
}

func (f *placeholderFile) GetSize() int64 {
	return f.size
}

func (f *placeholderFile) GetTime() time.Time {
	return f.time
}

func (f *placeholderFile) GetMeta() []byte {
	return nil
}

func (f *placeholderFile) GetHash() string {
	return f.hash
}

func (f *placeholderFile) IsDone() bool {
	return true
}

type hashFile struct {
	sts.File
	hash string
}

func (f *hashFile) GetHash() string {
	return f.hash
}

type recoverFile struct {
	sts.Cached
	prev string
	left []*sts.ByteRange
	part int
	used int64
}

func (f *recoverFile) GetPrev() string {
	return f.prev
}

func (f *recoverFile) Allocate(desired int64) (offset int64, length int64) {
	offset = f.left[f.part].Beg + f.used
	length = desired
	f.used += length
	if offset+length >= f.left[f.part].End {
		length = f.left[f.part].End - offset
		f.part++
		f.used = 0
	}
	return
}

func (f *recoverFile) IsAllocated() bool {
	return f.part == len(f.left)
}

func (f *recoverFile) GetSendSize() int64 {
	size := int64(0)
	for _, p := range f.left {
		size += p.End - p.Beg
	}
	return size
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
	prev      string
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

func (f *progressFile) GetPrev() string {
	return f.prev
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
