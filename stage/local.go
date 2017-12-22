package stage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/log"
)

const (
	compExt = ".cmp"
	partExt = ".part"

	// cacheAge is how long a file that is not part of a chain (i.e. order not
	// important) is kept in memory
	cacheAge = time.Hour * 24

	// cacheCnt is the number of files after which to age the cache
	cacheCnt = 1000

	// cacheMax is the maximum number of files to be kept in the cache
	// cacheMax = 10000

	// cacheMaxAge is the amount of time any file can live in the cache (even
	// if we think it's a predessor to some future file--we can't have it live
	// forever)
	cacheMaxAge = time.Hour * 24 * 30

	// waitPollCount is the number of times a file can be polled before a file
	// in waiting is released without any sign of its predecessor
	waitPollCount = 10

	// logSearch is how far back (max) to look in the logs for a finalized file
	logSearch = time.Duration(30 * 24 * time.Hour)

	stateReceived  = 0
	stateValidated = 1
	stateFailed    = 2
	stateFinalized = 3
	statePolled    = 4
)

func newCompanion(path string, file *sts.Partial) (cmp *sts.Partial, err error) {
	ext := filepath.Ext(path)
	if ext != compExt {
		path += compExt
	}
	if cmp, err = readCompanion(path, file.Name); err != nil || cmp != nil {
		return
	}
	cmp = &sts.Partial{
		Name:   file.Name,
		Prev:   file.Prev,
		Size:   file.Size,
		Hash:   file.Hash,
		Source: file.Source,
	}
	return
}

// readCompanion deserializes a companion file based on the input path.
func readCompanion(path, name string) (cmp *sts.Partial, err error) {
	ext := filepath.Ext(path)
	if ext != compExt {
		path += compExt
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	cmp = &sts.Partial{}
	err = fileutil.LoadJSON(path, cmp)
	if err != nil {
		// Try reading the old-style companion for backward compatibility
		oldCmp := &oldCompanion{}
		err = fileutil.LoadJSON(path, oldCmp)
		if err != nil {
			return
		}
		cmp.Hash = oldCmp.Hash
		cmp.Name = oldCmp.Path
		cmp.Prev = oldCmp.Prev
		cmp.Size = oldCmp.Size
		cmp.Source = oldCmp.Source
		if name != "" {
			// Because the old-style companion stored the full path and not
			// the name part
			cmp.Name = name
		}
		for _, p := range oldCmp.Parts {
			cmp.Parts = append(cmp.Parts, &sts.ByteRange{
				Beg: p.Beg, End: p.End,
			})
		}
	}
	return
}

func writeCompanion(path string, cmp *sts.Partial) error {
	ext := filepath.Ext(path)
	if ext != compExt {
		path += compExt
	}
	return fileutil.WriteJSON(path, cmp)
}

func addCompanionPart(cmp *sts.Partial, beg, end int64) {
	j := len(cmp.Parts)
	for i := 0; i < j; i++ {
		part := cmp.Parts[i]
		if beg >= part.End || end <= part.Beg {
			continue
		}
		log.Debug(fmt.Sprintf(
			"Remove Companion Conflict: %s => %d:%d (new) %d:%d (old)",
			cmp.Name, beg, end, part.Beg, part.End))
		cmp.Parts[j-1], cmp.Parts[i] = cmp.Parts[i], cmp.Parts[j-1]
		i--
		j--
	}
	cmp.Parts = cmp.Parts[:j]
	cmp.Parts = append(cmp.Parts, &sts.ByteRange{Beg: beg, End: end})
}

func isCompanionComplete(cmp *sts.Partial) bool {
	size := int64(0)
	for _, part := range cmp.Parts {
		size += part.End - part.Beg
	}
	return size == cmp.Size
}

type finalFile struct {
	path     string
	name     string
	prev     string
	size     int64
	hash     string
	source   string
	time     time.Time
	state    int
	nextDone bool
}

func (f *finalFile) GetName() string {
	return f.name
}

func (f *finalFile) GetSize() int64 {
	return f.size
}

func (f *finalFile) GetHash() string {
	return f.hash
}

// Stage is the manager for all things file reception
type Stage struct {
	rootDir   string
	targetDir string
	logger    sts.ReceiveLogger

	handlers   map[string]chan *finalFile
	poll       map[string]int
	pollLock   sync.RWMutex
	wait       map[string][]*finalFile
	waitLock   sync.RWMutex
	cache      map[string]*finalFile
	cacheByAge []*finalFile
	cacheLock  sync.RWMutex
	writeLock  sync.RWMutex
	writeLocks map[string]*sync.RWMutex
}

// New creates a new instance of Stage where the rootDir is the directory
// for the stage area (will append {source}/), targetDir is where files
// should be moved once validated, and logger instance for logging files
// received
func New(rootDir, targetDir string, logger sts.ReceiveLogger) *Stage {
	s := &Stage{
		rootDir:   rootDir,
		targetDir: targetDir,
		logger:    logger,
	}
	s.poll = make(map[string]int)
	s.wait = make(map[string][]*finalFile)
	s.cache = make(map[string]*finalFile)
	s.writeLocks = make(map[string]*sync.RWMutex)
	s.handlers = make(map[string]chan *finalFile)
	return s
}

func (s *Stage) getWriteLock(path string) *sync.RWMutex {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	var m *sync.RWMutex
	var exists bool
	if m, exists = s.writeLocks[path]; !exists {
		m = &sync.RWMutex{}
		s.writeLocks[path] = m
	}
	return m
}

func (s *Stage) delWriteLock(path string) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	delete(s.writeLocks, path)
}

func (s *Stage) hasWriteLock(key string) bool {
	s.writeLock.RLock()
	defer s.writeLock.RUnlock()
	_, ok := s.writeLocks[key]
	return ok
}

func (s *Stage) cmpPathToName(cmpPath string) (name string) {
	// Strip the root directory
	name = cmpPath[len(s.rootDir)+len(string(os.PathSeparator)):]
	// Trim the companion extension
	name = strings.TrimSuffix(name, compExt)
	// Strip the source
	name = filepath.Join(
		strings.Split(
			name, string(os.PathSeparator))[1:]...)
	return
}

// Scan walks the stage area tree (optionally restricted by source) looking
// for companion files and returns any found
func (s *Stage) Scan(source string) (partials []*sts.Partial, err error) {
	var name string
	var lock *sync.RWMutex
	root := filepath.Join(s.rootDir, source)
	err = filepath.Walk(root,
		func(path string, info os.FileInfo, err error) error {
			if filepath.Ext(path) == compExt {
				name = s.cmpPathToName(path)
				lock = s.getWriteLock(strings.TrimSuffix(path, compExt))
				lock.RLock()
				// Make sure it still exists.
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					cmp, err := readCompanion(path, name)
					if err == nil {
						partials = append(partials, cmp)
					}
				}
				lock.RUnlock()
			}
			return nil
		})
	return
}

func (s *Stage) initStageFile(path string, size int64) error {
	var err error
	info, err := os.Stat(path + partExt)
	if !os.IsNotExist(err) && info.Size() == size {
		return nil
	}
	if _, err = os.Stat(path + compExt); !os.IsNotExist(err) {
		log.Debug("Removing Stale Companion:", path+compExt)
		os.Remove(path + compExt)
	}
	log.Debug("Making Directory:", filepath.Dir(path))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}
	fh, err := os.Create(path + partExt)
	log.Debug(fmt.Sprintf("Creating Empty File: %s (%d B)", path, size))
	if err != nil {
		return fmt.Errorf(
			"Failed to create empty file at %s%s with size %d: %s",
			path, partExt, size, err.Error())
	}
	defer fh.Close()
	fh.Truncate(size)
	return nil
}

// Prepare is called with all binned parts of a request before each one is
// "Receive"d (below).  We want to initialize the files in the stage area.
func (s *Stage) Prepare(source string, parts []sts.Binned) {
	for _, part := range parts {
		path := filepath.Join(s.rootDir, source, part.GetName())
		lock := s.getWriteLock(path)
		lock.Lock()
		err := s.initStageFile(path, part.GetFileSize())
		lock.Unlock()
		if err != nil {
			log.Error(err.Error())
		}
	}
}

// Receive reads a single file part with file metadata and reader
func (s *Stage) Receive(file *sts.Partial, reader io.Reader) (err error) {
	if len(file.Parts) != 1 {
		err = fmt.Errorf(
			"Can only receive a single part for a single reader (%d given)",
			len(file.Parts))
		return
	}
	path := filepath.Join(s.rootDir, file.Source, file.Name)
	cached := s.fromCache(path)
	if cached != nil && cached.state != stateFailed && cached.hash == file.Hash {
		// This means we already received this file.  This can happen if a
		// request is interrupted after the last part of a file is received
		// but the sending side doesn't know what parts were received so it
		// sends the whole bin again.  It doesn't seem ideal to check the cache
		// for every file part but I think we need to in order to catch this
		// rare occurrence.
		log.Info("Ignoring duplicate:", file.Name)
		_, err = ioutil.ReadAll(reader)
		return
	}

	lock := s.getWriteLock(path)

	// Read the part and write it to the right place in the staged "partial"
	fh, err := os.OpenFile(path+partExt, os.O_WRONLY, 0600)
	if err != nil {
		err = fmt.Errorf("Failed to open file while trying to write part: %s",
			err.Error())
		return
	}
	part := file.Parts[0]
	fh.Seek(part.Beg, 0)
	_, err = io.Copy(fh, reader)
	fh.Close()
	if err != nil {
		return
	}

	// Make sure we're the only one updating the companion
	lock.Lock()
	defer lock.Unlock()

	cmp, err := newCompanion(path, file)
	if err != nil {
		return
	}

	addCompanionPart(cmp, part.Beg, part.End)
	if err = writeCompanion(path, cmp); err != nil {
		err = fmt.Errorf("Failed to write updated companion: %s", err.Error())
		return
	}

	done := isCompanionComplete(cmp)
	if done {
		final := s.partialToFinal(file)
		s.toCache(final, stateReceived)
		if err = os.Rename(path+partExt, path); err != nil {
			err = fmt.Errorf("Failed to drop \"partial\" extension: %s",
				err.Error())
			return
		}
		log.Debug("File transmitted:", path)
		s.delWriteLock(path)
		go s.process(final)
	}
	return
}

// GetFileStatus returns the status of a file based on its source, name, and
// time sent
//
// It returns one of these constants:
// -> sts.ConfirmPassed: MD5 validation was successful and file put away
// -> sts.ConfirmFailed: MD5 validation was unsuccessful and file should be resent
// -> sts.ConfirmWaiting: MD5 validation was successful but waiting on predecessor
// -> sts.ConfirmNone: No knowledge of file
func (s *Stage) GetFileStatus(source, relPath string, sent time.Time) int {
	log.Debug("Checking status:", source, relPath)
	path := filepath.Join(s.rootDir, source, relPath)
	f := s.fromCache(path)
	if f == nil {
		if s.logger.WasReceived(source, relPath, sent, time.Now()) {
			f = &finalFile{
				path:   path,
				name:   relPath,
				source: source,
			}
			goto confirmPassed
		}
		return sts.ConfirmNone
	}
	switch f.state {
	case stateFailed:
		return sts.ConfirmFailed
	case stateValidated:
		file := s.getWaiting(path)
		if file != nil {
			go s.finalizeQueue(file)
			s.incrementPollCount(path)
			return sts.ConfirmWaiting
		}
	case stateFinalized:
		goto confirmPassed
	}
	return sts.ConfirmNone
confirmPassed:
	s.removePoll(path)
	s.toCache(f, statePolled)
	return sts.ConfirmPassed
}

// Recover is meant to be run while the server is not so it can cleanly address
// files in the stage area that should be completed from the previous server
// run
func (s *Stage) Recover() (err error) {
	log.Debug("Recovering...")
	defer log.Debug("Recovery complete")
	var ready []*sts.Partial
	err = filepath.Walk(s.rootDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if filepath.Ext(path) != compExt {
				return nil
			}
			cmp, err := readCompanion(path, s.cmpPathToName(path))
			if err != nil {
				log.Error("Failed to read companion:", path, err.Error())
				return nil
			}
			base := strings.TrimSuffix(path, compExt)
			if _, err = os.Stat(base + partExt); !os.IsNotExist(err) {
				if isCompanionComplete(cmp) {
					if err = os.Rename(base+partExt, base); err != nil {
						log.Error("Failed to drop \"partial\" extension:",
							err.Error())
						return nil
					}
					log.Debug("Found already done:", cmp.Name)
					ready = append(ready, cmp)
				}
			} else if _, err = os.Stat(base); os.IsNotExist(err) {
				if err = os.Remove(path); err != nil {
					log.Error("Failed to remove orphaned companion:",
						path, err.Error())
					return nil
				}
				log.Info("Removed orphaned companion:", path)
			} else if isCompanionComplete(cmp) {
				log.Debug("Found already done:", cmp.Name)
				ready = append(ready, cmp)
			}
			return nil
		})
	if len(ready) == 0 {
		return
	}
	// Build the cache from the log
	s.cacheLock.Lock() // Shouldn't be necessary but just to be safe
	s.logger.Parse(func(source, name, hash string, size int64, t time.Time) bool {
		path := filepath.Join(s.rootDir, source, name)
		file := &finalFile{
			source: source,
			path:   path,
			name:   name,
			hash:   hash,
			size:   size,
			time:   t,
			state:  stateFinalized,
		}
		s.cache[path] = file
		s.cacheByAge = append(s.cacheByAge, file)
		log.Debug("Log to cache:", source, file.name)
		return false
	}, time.Now().Add(-1*cacheMaxAge), time.Now())
	s.cacheLock.Unlock()
	worker := func(wg *sync.WaitGroup, ch <-chan *sts.Partial) {
		defer wg.Done()
		for f := range ch {
			s.process(s.partialToFinal(f))
		}
	}
	wg := sync.WaitGroup{}
	ch := make(chan *sts.Partial)
	// 16 workers is somewhat arbitrary--we just want some notion of doing
	// things concurrently.  We don't want to just have them all go off because
	// we could bump against the OS's threshold for max files open at once.
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go worker(&wg, ch)
	}
	for _, file := range ready {
		ch <- file
	}
	close(ch)
	wg.Wait()
	return
}

func (s *Stage) partialToFinal(file *sts.Partial) *finalFile {
	return &finalFile{
		path:   filepath.Join(s.rootDir, file.Source, file.Name),
		name:   file.Name,
		size:   file.Size,
		hash:   file.Hash,
		prev:   file.Prev,
		source: file.Source,
	}
}

func (s *Stage) process(file *finalFile) {
	log.Debug("Validating:", file.name)

	// Validate checksum.
	hash, err := fileutil.FileMD5(file.path)
	if err != nil {
		os.Remove(file.path + compExt)
		os.Remove(file.path)
		log.Error(fmt.Sprintf(
			"Failed to calculate MD5 of %s: %s",
			file.name, err.Error()))
		return
	}

	valid := file.hash == hash

	if !valid {
		log.Error(fmt.Sprintf("Failed validation: %s (%s => %s)",
			file.path, file.hash, hash))
		s.toCache(file, stateFailed)
		return
	}

	s.toCache(file, stateValidated)

	log.Debug("Validated:", file.name)
	s.finalizeQueue(file)
}

func (s *Stage) finalizeQueue(file *finalFile) {
	var ch chan *finalFile
	var ok bool
	if ch, ok = s.handlers[file.source]; !ok {
		ch = make(chan *finalFile, 100)
		s.handlers[file.source] = ch
		go s.finalizeHandler(ch)
	}
	ch <- file
}

func (s *Stage) finalizeHandler(ch <-chan *finalFile) {
	for f := range ch {
		s.finalizeChain(f)
	}
}

func (s *Stage) finalizeChain(file *finalFile) {
	log.Debug("Finalize chain:", file.name)

	// It's possible to get duplicates so we can use the cache to identify
	// those and safely ignore them.
	if f := s.fromCache(file.path); f != nil && f.state == stateFinalized {
		if file.hash == f.hash {
			log.Debug("File already finalized:", file.name)
			return
		}
	}

	done, err := s.finalize(file)
	if err != nil {
		log.Error(err)
		return
	}
	if done {
		waiting := s.fromWait(file.path)
		for _, waitFile := range waiting {
			log.Debug("Stage found waiting:", waitFile.name, "<-", file.name)
			go s.finalizeQueue(waitFile)
		}
	}
}

func (s *Stage) finalize(file *finalFile) (done bool, err error) {
	// Make sure it doesn't need to wait in line.
	if file.prev != "" {
		stagedPrevFile := filepath.Join(s.rootDir, file.source, file.prev)
		prev := s.fromCache(stagedPrevFile)
		switch {
		case prev == nil:
			if s.hasWriteLock(stagedPrevFile) {
				log.Debug(
					"Previous file in progress:",
					file.name, "<-", file.prev)
				break
			}
			pollCount := s.getPollCount(file.path)
			if pollCount <= waitPollCount {
				log.Debug(
					"Previous file not found yet:",
					file.name, "<-", file.prev)
				break
			}
			// If this file has been polled waitPollCount times and we still
			// can't find any trace of its predecessor (which is the case
			// if we get here) then release it anyway at the risk of
			// getting data out of order. We can only do so much.
			log.Info("Done Waiting:", file.name, "<-", file.prev)
			s.doneWaiting(stagedPrevFile, file)
			goto finalize

		case prev.state == stateReceived:
			log.Debug("Waiting for previous file:",
				file.name, "<-", file.prev)

		case prev.state == stateFailed:
			log.Debug("Previous file failed:",
				file.name, "<-", file.prev)

		case prev.state == stateValidated:
			if s.isWaiting(stagedPrevFile) {
				log.Debug("Previous file waiting:",
					file.name, "<-", file.prev)
			} else {
				log.Debug("Previous file finalizing:",
					file.name, "<-", file.prev)
			}
		default:
			goto finalize
		}
		s.removePoll(file.path)
		s.toWait(stagedPrevFile, file)
		return
	}

finalize:
	log.Debug("Finalizing", file.source, file.name)

	// Let's log it and update the cache before actually putting the file away
	// in order to properly recover in case something happens between now and
	// then.
	s.logger.Received(file.source, file)

	s.toCache(file, stateFinalized)

	// Move it.
	targetPath := filepath.Join(s.targetDir, file.source, file.name)
	os.MkdirAll(filepath.Dir(targetPath), 0775)
	if err = fileutil.Move(file.path, targetPath); err != nil {
		err = fmt.Errorf(
			"Failed to move %s to %s: %s",
			file.path,
			targetPath+fileutil.LockExt,
			err.Error())
		return
	}

	// Clean up the companion.
	if err = os.Remove(file.path + compExt); err != nil {
		err = fmt.Errorf(
			"Failed to remove companion file for %s: %s",
			file.path, err.Error())
		return
	}

	log.Debug("Finalized", file.source, file.name)

	done = true
	return
}

func (s *Stage) getPollCount(path string) int {
	s.pollLock.RLock()
	defer s.pollLock.RUnlock()
	if count, ok := s.poll[path]; ok {
		return count
	}
	return 0
}

func (s *Stage) incrementPollCount(path string) (c int) {
	s.pollLock.Lock()
	defer s.pollLock.Unlock()
	var ok bool
	if c, ok = s.poll[path]; ok {
		c++
		s.poll[path] = c
		return
	}
	c = 1
	s.poll[path] = c
	return
}

func (s *Stage) removePoll(path string) {
	s.pollLock.Lock()
	defer s.pollLock.Unlock()
	if _, ok := s.poll[path]; ok {
		delete(s.poll, path)
	}
}

func (s *Stage) isWaiting(path string) bool {
	return s.getWaiting(path) != nil
}

// getWaiting returns a finalFile instance currently waiting on its predecessor
func (s *Stage) getWaiting(path string) *finalFile {
	s.waitLock.RLock()
	defer s.waitLock.RUnlock()
	for _, files := range s.wait {
		for _, file := range files {
			if file.path == path {
				return file
			}
		}
	}
	return nil
}

// doneWaiting removes a file from the wait cache that is waiting on its
// predessor
func (s *Stage) doneWaiting(prevPath string, next *finalFile) {
	s.waitLock.Lock()
	defer s.waitLock.Unlock()
	files, ok := s.wait[prevPath]
	if !ok {
		return
	}
	var rest []*finalFile
	for _, file := range files {
		if file.path == next.path {
			continue
		}
		rest = append(rest, file)
	}
	if len(rest) > 0 {
		s.wait[prevPath] = rest
		return
	}
	delete(s.wait, prevPath)
}

// fromWait returns the file(s) currently waiting on the file indicated by path
func (s *Stage) fromWait(prevPath string) []*finalFile {
	s.waitLock.Lock()
	defer s.waitLock.Unlock()
	files, ok := s.wait[prevPath]
	if ok {
		delete(s.wait, prevPath)
		return files
	}
	return nil
}

func (s *Stage) toWait(prevPath string, next *finalFile) {
	s.waitLock.Lock()
	defer s.waitLock.Unlock()
	files, ok := s.wait[prevPath]
	if ok {
		for _, waiting := range files {
			if waiting.path == next.path {
				return
			}
		}
		files = append(files, next)
	} else {
		files = []*finalFile{next}
	}
	s.wait[prevPath] = files
}

func (s *Stage) fromCache(path string) *finalFile {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	file, _ := s.cache[path]
	return file
}

func (s *Stage) toCache(file *finalFile, state int) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	file.time = time.Now()
	file.state = state
	if f, ok := s.cache[file.path]; ok && f.nextDone && f.hash == file.hash {
		// Only keep the nextDone value if this really is the same file just
		// a different object in memory
		file.nextDone = true
	}
	s.cache[file.path] = file
	if len(s.cache)%cacheCnt == 0 {
		go s.cleanCache()
	}
	if state < statePolled || file.prev == "" {
		return
	}
	// If it has a previous file, mark that file to be eligible for removal
	// from the cache since this one is the latest in the chain
	prevPath := filepath.Join(s.rootDir, file.source, file.prev)
	if prev, ok := s.cache[prevPath]; ok {
		prev.nextDone = true
	}
}

func (s *Stage) cleanCache() {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	log.Debug("Cleaning cache...", len(s.cache))
	var age time.Duration
	for _, cacheFile := range s.cache {
		age = time.Since(cacheFile.time)
		if age > cacheMaxAge {
			delete(s.cache, cacheFile.path)
			continue
		}
		if cacheFile.state < statePolled {
			continue
		}
		if !cacheFile.nextDone && age < cacheAge {
			continue
		}
		delete(s.cache, cacheFile.path)
	}
}

type oldCompanion struct {
	Path   string              `json:"path"`
	Prev   string              `json:"prev"`
	Size   int64               `json:"size"`
	Hash   string              `json:"hash"`
	Source string              `json:"src"`
	Parts  map[string]*oldPart `json:"parts"`
}

type oldPart struct {
	Hash string `json:"hash"`
	Beg  int64  `json:"b"`
	End  int64  `json:"e"`
}
