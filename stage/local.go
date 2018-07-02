package stage

import (
	"encoding/json"
	"fmt"
	"io"
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
	fullExt = ".full"
	waitExt = ".wait"

	// cacheAge is how long a received file is cached in memory
	cacheAge = time.Hour * 1

	// cacheCnt is the number of files after which to age the cache
	cacheCnt = 1000

	stateReceived  = 0
	stateValidated = 1
	stateFailed    = 2
	stateFinalized = 3
	stateLogged    = 4
)

type finalFile struct {
	path   string
	name   string
	prev   string
	size   int64
	hash   string
	source string
	time   time.Time
	logged time.Time
	state  int
	wait   *time.Timer
	nErr   int
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
	name      string
	rootDir   string
	targetDir string
	logger    sts.ReceiveLogger
	nPipe     int

	cleaned    time.Time
	validateCh chan *finalFile
	finalizeCh chan *finalFile
	wait       map[string][]*finalFile
	waitLock   sync.RWMutex
	cacheLock  sync.RWMutex
	cache      map[string]*finalFile
	cacheTime  time.Time
	writeLock  sync.RWMutex
	writeLocks map[string]*sync.RWMutex
}

// New creates a new instance of Stage where the rootDir is the directory
// for the stage area (will append {source}/), targetDir is where files
// should be moved once validated, and logger instance for logging files
// received
func New(name, rootDir, targetDir string, logger sts.ReceiveLogger) *Stage {
	s := &Stage{
		name:      name,
		rootDir:   rootDir,
		targetDir: targetDir,
		logger:    logger,
	}
	s.wait = make(map[string][]*finalFile)
	s.cache = make(map[string]*finalFile)
	s.writeLocks = make(map[string]*sync.RWMutex)
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

func (s *Stage) pathToName(path, stripExt string) (name string) {
	// Strip the root directory
	name = path[len(s.rootDir)+len(string(os.PathSeparator)):]
	if stripExt != "" {
		// Trim the extension
		name = strings.TrimSuffix(name, stripExt)
	}
	return
}

// Scan walks the stage area tree looking for companion files and returns any
// found in the form of a JSON-encoded byte array
func (s *Stage) Scan(version string) (jsonBytes []byte, err error) {
	var name string
	var lock *sync.RWMutex
	var partials []*sts.Partial
	err = filepath.Walk(s.rootDir,
		func(path string, info os.FileInfo, err error) error {
			if filepath.Ext(path) == compExt {
				name = s.pathToName(path, compExt)
				lock = s.getWriteLock(strings.TrimSuffix(path, compExt))
				lock.RLock()
				// Make sure it still exists.
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					cmp, err := readLocalCompanion(path, name)
					if err == nil {
						partials = append(partials, cmp)
					}
				}
				lock.RUnlock()
			}
			return nil
		})
	if err != nil {
		return
	}
	switch version {
	case "":
		jsonBytes, err = json.Marshal(toLegacyCompanions(partials))
	default:
		jsonBytes, err = json.Marshal(partials)
	}
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
func (s *Stage) Prepare(parts []sts.Binned) {
	for _, part := range parts {
		path := filepath.Join(s.rootDir, part.GetName())
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
	path := filepath.Join(s.rootDir, file.Name)
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

	cmp, err := newLocalCompanion(path, file)
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
		if err = os.Rename(path+partExt, path+fullExt); err != nil {
			err = fmt.Errorf("Failed to swap in the \"full\" extension: %s",
				err.Error())
			return
		}
		log.Debug("File transmitted:", path)
		s.delWriteLock(path)
		go s.processQueue(final)
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
func (s *Stage) GetFileStatus(relPath string, sent time.Time) int {
	s.buildCache(sent)
	path := filepath.Join(s.rootDir, relPath)
	f := s.fromCache(path)
	if f == nil {
		log.Debug("STAGE:", s.name, relPath, "(not found)")
		return sts.ConfirmNone
	}
	switch f.state {
	case stateFailed:
		log.Debug("STAGE:", s.name, relPath, "(failed)")
		return sts.ConfirmFailed
	case stateValidated:
		file := s.getWaiting(path)
		if file != nil {
			log.Debug("STAGE:", s.name, relPath, "(waiting)")
			return sts.ConfirmWaiting
		}
		log.Debug("STAGE:", s.name, relPath, "(done)")
		return sts.ConfirmPassed
	case stateLogged:
		log.Debug("STAGE:", s.name, relPath, "(logged)")
		return sts.ConfirmPassed
	case stateFinalized:
		log.Debug("STAGE:", s.name, relPath, "(done)")
		return sts.ConfirmPassed
	}
	log.Debug("STAGE:", s.name, relPath, "(not found)")
	return sts.ConfirmNone
}

// Recover is meant to be run while the server is not so it can cleanly address
// files in the stage area that should be completed from the previous server
// run
func (s *Stage) Recover() (err error) {
	log.Info("Beginning stage recovery")
	defer log.Info("Stage recovery complete")
	var validate []*sts.Partial
	var finalize []*sts.Partial
	oldest := time.Now()
	err = filepath.Walk(s.rootDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if filepath.Ext(path) != compExt {
				return nil
			}
			cmp, err := readLocalCompanion(path, s.pathToName(path, compExt))
			if err != nil {
				log.Error("Failed to read companion:", path, err.Error())
				return nil
			}
			if info.ModTime().Before(oldest) {
				oldest = info.ModTime()
			}
			base := strings.TrimSuffix(path, compExt)
			if _, err = os.Stat(base + waitExt); !os.IsNotExist(err) {
				// .wait
				log.Debug("Found ready to finalize:", cmp.Name)
				finalize = append(finalize, cmp)
			} else if _, err = os.Stat(base + fullExt); !os.IsNotExist(err) {
				// .full
				log.Debug("Found ready to validate:", cmp.Name)
				validate = append(validate, cmp)
			} else if _, err = os.Stat(base + partExt); !os.IsNotExist(err) {
				// .part
				if isCompanionComplete(cmp) {
					if err = os.Rename(base+partExt, base+fullExt); err != nil {
						log.Error("Failed to swap in \"full\" extension:",
							err.Error())
						return nil
					}
					log.Debug("Found already done:", cmp.Name)
					validate = append(validate, cmp)
				}
			} else if _, err = os.Stat(base); os.IsNotExist(err) {
				// Not found
				if err = os.Remove(path); err != nil {
					log.Error("Failed to remove orphaned companion:",
						path, err.Error())
					return nil
				}
				log.Info("Removed orphaned companion:", path)
			} else if isCompanionComplete(cmp) {
				// No extension (backward compatibility)
				if err = os.Rename(base, base+fullExt); err != nil {
					log.Error("Failed to add \"full\" extension:",
						err.Error())
					return nil
				}
				log.Debug("Found ready to validate:", cmp.Name)
				validate = append(validate, cmp)
			}
			return nil
		})
	if len(validate) == 0 && len(finalize) == 0 {
		return
	}
	// Build the cache from the incoming log starting an hour before the oldest
	// companion file found
	s.buildCache(oldest.Add(-1 * time.Hour))
	if len(validate) > 0 {
		worker := func(wg *sync.WaitGroup, ch <-chan *sts.Partial) {
			defer wg.Done()
			for f := range ch {
				s.process(s.partialToFinal(f))
			}
		}
		wg := sync.WaitGroup{}
		ch := make(chan *sts.Partial)
		// 32 workers is somewhat arbitrary--we just want some notion of doing
		// things concurrently.  We don't want to just have them all go off because
		// we could bump against the OS's threshold for max files open at once.
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go worker(&wg, ch)
		}
		for _, file := range validate {
			ch <- file
		}
		close(ch)
		wg.Wait()
	}
	for _, file := range finalize {
		finalFile := s.partialToFinal(file)
		s.toCache(finalFile, stateValidated)
		s.finalizeQueue(finalFile)
	}
	return
}

// Stop waits for the number of files in the pipe to be zero and then signals
func (s *Stage) Stop(wg *sync.WaitGroup) {
	defer wg.Done()
	log.Debug("Stage shutting down ...")
	for {
		if s.inPipe() > 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
}

func (s *Stage) partialToFinal(file *sts.Partial) *finalFile {
	return &finalFile{
		path:   filepath.Join(s.rootDir, file.Name),
		name:   file.Name,
		size:   file.Size,
		hash:   file.Hash,
		prev:   file.Prev,
		source: file.Source,
	}
}

func (s *Stage) processQueue(file *finalFile) {
	if s.validateCh == nil {
		s.validateCh = make(chan *finalFile, 100)
		// Let's not have too many files processed at once
		for i := 0; i < 16; i++ {
			go s.processHandler()
		}
	}
	s.validateCh <- file
}

func (s *Stage) processHandler() {
	for f := range s.validateCh {
		s.process(f)
	}
}

func (s *Stage) process(file *finalFile) {
	log.Debug("Validating:", file.name)

	// Validate checksum.
	hash, err := fileutil.FileMD5(file.path + fullExt)
	if err != nil {
		os.Remove(file.path + compExt)
		os.Remove(file.path + fullExt)
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

	if err = os.Rename(file.path+fullExt, file.path+waitExt); err != nil {
		err = fmt.Errorf("Failed to swap in \"wait\" extension: %s",
			err.Error())
		s.toCache(file, stateFailed)
		return
	}

	s.toCache(file, stateValidated)

	log.Debug("Validated:", file.name)
	s.finalizeQueue(file)
}

func (s *Stage) finalizeQueue(file *finalFile) {
	if s.finalizeCh == nil {
		s.finalizeCh = make(chan *finalFile, 100)
		go s.finalizeHandler()
	}
	s.finalizeCh <- file
}

func (s *Stage) finalizeHandler() {
	d := time.Second * 10
	t := time.NewTimer(d)
	for {
		select {
		case f := <-s.finalizeCh:
			t.Stop()
			s.finalizeChain(f)
		case <-t.C:
			// If there's not an active channel and it's been long enough
			// since our last directory cleaning, kick one off
			if s.cleaned.IsZero() || time.Since(s.cleaned) > cacheAge {
				s.cleaned = time.Now()
				// Not comfortable enough yet to enable automatic stage area
				// cleaning.  I don't like that a file gets logged as received
				// before moving it because in theory it means that a file
				// could be waiting in the stage area but the fact that it was
				// read from the received log could potentially mean that a
				// file could be deleted that wasn't yet moved.  Better safe
				// than sorry.
				// go s.cleanDir()
			}
		}
		t.Reset(d)
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
	// Make sure it doesn't need to wait in line
	if file.prev != "" {
		stagedPrevFile := filepath.Join(s.rootDir, file.prev)
		prev := s.fromCache(stagedPrevFile)
		switch {
		case prev == nil:
			if s.hasWriteLock(stagedPrevFile) {
				log.Debug(
					"Previous file in progress:",
					file.name, "<-", file.prev)
				break
			}

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
				if s.isWaitLoop(stagedPrevFile) {
					goto finalize
				}
				break
			}
			log.Debug("Previous file finalizing:",
				file.name, "<-", file.prev)
		default:
			goto finalize
		}
		s.toWait(stagedPrevFile, file)
		// Set a timer to check this file again in case there is a wait loop
		file.wait = time.AfterFunc(time.Second*30, func() {
			log.Debug("Attempting finalize again:", file.name)
			s.finalizeQueue(file)
		})
		return
	}

finalize:
	if file.wait != nil {
		file.wait.Stop()
		file.wait = nil
	}

	log.Debug("Finalizing", file.source, file.name)
	if err = s.putFileAway(file); err != nil {
		return
	}
	log.Debug("Finalized", file.source, file.name)
	done = true
	return
}

func (s *Stage) putFileAway(file *finalFile) (err error) {
	// Better to log the file twice rather than receive it twice.  If we log
	// after putting the file away, it's possible that a crash could occur
	// after putting the file away but before logging. On restart, there would
	// be no knowledge that the file was received and it would be sent again.
	s.logger.Received(file)
	file.logged = time.Now()

	// Move it
	targetPath := filepath.Join(s.targetDir, file.name)
	os.MkdirAll(filepath.Dir(targetPath), 0775)
	if err = fileutil.Move(file.path+waitExt, targetPath); err != nil {
		err = fmt.Errorf(
			"Failed to move %s to %s: %s",
			file.path+waitExt, targetPath, err.Error())
		// If the file doesn't exist then something is really wrong.
		// Either we somehow have two instances running that are stepping
		// on each other or we have an illusive and critical bug.  Regardless,
		// it's not good.
		if !os.IsNotExist(err) {
			// If the file is there but we still got an error, let's just try
			// it again later.
			file.nErr++
			time.AfterFunc(time.Second*time.Duration(file.nErr), func() {
				log.Debug("Attempting finalize again after failure:", file.name)
				s.finalizeQueue(file)
			})
		}
		return
	}

	s.toCache(file, stateFinalized)

	// Clean up the companion (no need to capture an error since it wouldn't
	// be a deal-breaker anyway)
	os.Remove(file.path + compExt)
	return
}

func (s *Stage) isWaiting(path string) bool {
	return s.getWaiting(path) != nil
}

// isWaitLoop recurses through the wait tree to see if there is a file that
// points back to the original, thus indicating a loop
func (s *Stage) isWaitLoop(prevPath string) bool {
	s.waitLock.RLock()
	defer s.waitLock.RUnlock()
	paths := []string{prevPath}
	var next []string
	for {
		for _, p := range paths {
			ff, ok := s.wait[p]
			if !ok {
				continue
			}
			for _, f := range ff {
				if f.path == prevPath {
					log.Debug("Wait loop detected:", prevPath, "<-", p)
					return true
				}
				next = append(next, f.path)
			}
		}
		if next == nil {
			break
		}
		paths = next
		next = nil
	}
	return false
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

func (s *Stage) inPipe() int {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	return s.nPipe
}

func (s *Stage) toCache(file *finalFile, state int) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	file.time = time.Now()
	if state != stateLogged {
		// This is to keep track of how many files are in the "pipe"
		if _, ok := s.cache[file.path]; !ok {
			s.nPipe++
		} else if state == stateFinalized {
			s.nPipe--
		}
	}
	file.state = state
	if !file.logged.IsZero() {
		if s.cacheTime.IsZero() {
			s.cacheTime = file.logged
		}
	}
	s.cache[file.path] = file
	if len(s.cache)%cacheCnt == 0 {
		go s.cleanCache()
	}
}

func (s *Stage) buildCache(from time.Time) {
	if func(mux *sync.RWMutex, t0 time.Time, t1 time.Time) bool {
		mux.RLock()
		defer mux.RUnlock()
		return t0.After(t1)
	}(&s.cacheLock, s.cacheTime, from) {
		return
	}
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	var cacheTime time.Time
	if s.cacheTime.IsZero() {
		cacheTime = time.Now()
	} else if from.After(cacheTime) {
		return
	}
	log.Debug("Building cache from logs...", s.name, from)
	var first time.Time
	s.logger.Parse(func(name, hash string, size int64, t time.Time) bool {
		if t.After(cacheTime) {
			return true
		}
		if first.IsZero() {
			first = t
		}
		path := filepath.Join(s.rootDir, name)
		if f, ok := s.cache[path]; ok && t.Before(f.logged) {
			// Skip it if a newer file of this name is already in the cache
			return false
		}
		file := &finalFile{
			source: s.name,
			path:   path,
			name:   name,
			hash:   hash,
			size:   size,
			time:   time.Now(),
			logged: t,
			state:  stateLogged,
		}
		s.cache[path] = file
		log.Debug("Cached from log:", s.name, file.name)
		return false
	}, from, cacheTime)
	s.cacheTime = first
}

func (s *Stage) cleanCache() {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	log.Debug("Cleaning cache...", len(s.cache))
	var age time.Duration
	s.cacheTime = time.Now()
	for _, cacheFile := range s.cache {
		age = time.Since(cacheFile.time)
		if cacheFile.state > stateFinalized {
			if age > cacheAge {
				delete(s.cache, cacheFile.path)
				log.Debug("Removed from cache:",
					cacheFile.source, cacheFile.name)
				continue
			}
			// We want the source cacheTime to be the earliest logged time of
			// the files still in the cache
			if cacheFile.logged.Before(s.cacheTime) {
				s.cacheTime = cacheFile.logged
			}
		}
	}
}

// cleanDir scans the staging directory looking for stale files that can be
// cleaned up.  There is at least one legitimate reason for files to become
// stale in the staging area: a request is interrupted after the last
// remaining part of a file is received but before the sending end can be
// notified which part(s) were properly received and the sending side merely
// sends the whole request again and at least part of a file is created in the
// staging area redundantly and never picked up by the stage.
func (s *Stage) cleanDir() {
	t := time.Now()
	aged := make(map[string][]string)
	filepath.Walk(s.rootDir,
		func(path string, info os.FileInfo, err error) error {
			if time.Now().Add(-1 * cacheAge).Before(info.ModTime()) {
				return nil
			}
			if info.ModTime().Before(t) {
				t = info.ModTime()
			}
			p := path
			ext := filepath.Ext(path)
			switch ext {
			case compExt:
				fallthrough
			case partExt:
				fallthrough
			case fullExt:
				fallthrough
			case waitExt:
				p = strings.TrimSuffix(path, ext)
			}
			if _, ok := aged[p]; !ok {
				aged[p] = []string{path}
				return nil
			}
			aged[p] = append(aged[p], path)
			return nil
		})
	if len(aged) == 0 {
		return
	}
	s.buildCache(t)
	for key, paths := range aged {
		f := s.fromCache(key)
		if f == nil {
			continue
		}
		if f.state > stateFinalized {
			for _, p := range paths {
				log.Info("Removing stale stage file:", p)
				if err := os.Remove(p); err != nil {
					log.Error("Failed to remove stale stage file:",
						err.Error())
				}
			}
		}
	}
}
