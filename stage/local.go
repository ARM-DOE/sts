package stage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/fileutil"
	"github.com/arm-doe/sts/log"
)

const (
	compExt = ".cmp"
	partExt = ".part"
	fullExt = ".full"
	waitExt = ".wait"

	// cacheAgeLogged is the minimum amount of time a received file is cached in
	// memory based on when it was logged relative to now
	cacheAgeLogged = time.Hour * 24

	// cacheAgeLoaded is the minimum amount of time a received file is cached in
	// memory based on when it was cached relative to now
	cacheAgeLoaded = time.Hour * 1

	// cacheCnt is the number of files received after which to age the cache
	cacheCnt = 1000

	// nValidators is the number of concurrent goroutines doing hash validation
	nValidators = 24

	stateUnknown   = -1
	stateReceived  = 0
	stateValidated = 1
	stateFailed    = 2
	stateFinalized = 3
	stateLogged    = 4
)

type finalFile struct {
	path        string
	name        string
	renamed     string
	prev        string
	size        int64
	hash        string
	time        time.Time
	logged      time.Time
	state       int
	wait        *time.Timer
	nErr        int
	prevScan    time.Time
	prevScanBeg time.Time
	nextFinal   bool
}

func (f *finalFile) GetName() string {
	return f.name
}

func (f *finalFile) GetRenamed() string {
	return f.renamed
}

func (f *finalFile) GetSize() int64 {
	return f.size
}

func (f *finalFile) GetHash() string {
	return f.hash
}

func (f *finalFile) GetBody() string {
	return f.path
}

// Stage is the manager for all things file reception
type Stage struct {
	name          string
	rootDir       string
	targetDir     string
	logger        sts.ReceiveLogger
	dispatcher    sts.Dispatcher
	exporter      sts.Exporter
	nPipe         int
	lastIn        time.Time
	cleanInterval time.Duration
	cleanTimeout  *time.Timer
	canReceive    bool

	validateCh chan *finalFile
	finalizeCh chan *finalFile
	wait       map[string][]*finalFile
	waitLock   sync.RWMutex
	cacheLock  sync.RWMutex
	cache      map[string]*finalFile
	cacheTime  time.Time
	cacheTimes []time.Time

	pathLock  sync.RWMutex
	pathLocks map[string]*sync.RWMutex
	readyLock sync.RWMutex
	cleanLock sync.RWMutex
}

// New creates a new instance of Stage where the rootDir is the directory
// for the stage area (will append {source}/), targetDir is where files
// should be moved once validated, and logger instance for logging files
// received
func New(
	name, rootDir, targetDir string,
	logger sts.ReceiveLogger,
	dispatcher sts.Dispatcher,
	exporter sts.Exporter,
) *Stage {
	s := &Stage{
		name:       name,
		rootDir:    rootDir,
		targetDir:  targetDir,
		logger:     logger,
		dispatcher: dispatcher,
		exporter:   exporter,
	}
	s.wait = make(map[string][]*finalFile)
	s.cache = make(map[string]*finalFile)
	s.pathLocks = make(map[string]*sync.RWMutex)
	s.lastIn = time.Now()
	s.cleanInterval = time.Minute * 30
	s.canReceive = true
	s.scheduleClean()
	s.validateCh = make(chan *finalFile, 100)
	// Let's not have too many files processed at once
	for i := 0; i < nValidators; i++ {
		go s.processHandler()
	}
	s.finalizeCh = make(chan *finalFile, 100)
	go s.finalizeHandler()
	return s
}

func (s *Stage) getPathLock(key string) *sync.RWMutex {
	// s.logDebug("Getting path lock for:", key)
	// defer s.logDebug("Got path lock for:", key)
	s.pathLock.Lock()
	defer s.pathLock.Unlock()
	var m *sync.RWMutex
	var exists bool
	if m, exists = s.pathLocks[key]; !exists {
		m = &sync.RWMutex{}
		s.pathLocks[key] = m
	}
	return m
}

func (s *Stage) delPathLock(key string) {
	// s.logDebug("Deleting path lock for:", key)
	// defer s.logDebug("Deleted path lock for:", key)
	s.pathLock.Lock()
	defer s.pathLock.Unlock()
	delete(s.pathLocks, key)
	s.lastIn = time.Now()
	s.logDebug("Write Locks:", len(s.pathLocks))
}

func (s *Stage) hasPathLock(key string) bool {
	s.pathLock.RLock()
	defer s.pathLock.RUnlock()
	_, ok := s.pathLocks[key]
	return ok
}

// func (s *Stage) getLastIn() time.Time {
// 	s.pathLock.RLock()
// 	defer s.pathLock.RUnlock()
// 	return s.lastIn
// }

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
				lock = s.getPathLock(strings.TrimSuffix(path, compExt))
				lock.RLock()
				// Make sure it still exists.
				if _, err := os.Stat(path); err == nil {
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
	if err == nil && info.Size() == size {
		return nil
	}
	if _, err = os.Stat(path + compExt); !os.IsNotExist(err) {
		cachedState := s.getFileState(path)
		// In case some catastrophe causes the sender to keep sending the same
		// file, at least we won't be clobbering legitimate companion files.
		if cachedState == stateUnknown || cachedState == stateFailed {
			s.logDebug("Removing Stale Companion:", path+compExt)
			os.Remove(path + compExt)
		}
	}
	s.logDebug("Making Directory:", filepath.Dir(path))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}
	fh, err := os.Create(path + partExt)
	s.logDebug(fmt.Sprintf("Creating Empty File: %s (%d B)", path, size))
	if err != nil {
		return fmt.Errorf(
			"failed to create empty file at %s%s with size %d: %s",
			path, partExt, size, err.Error())
	}
	defer fh.Close()
	return fh.Truncate(size)
}

// Prepare is called with all binned parts of a request before each one is
// "Receive"d (below).  We want to initialize the files in the stage area.
func (s *Stage) Prepare(parts []sts.Binned) {
	for _, part := range parts {
		path := filepath.Join(s.rootDir, part.GetName())
		lock := s.getPathLock(path)
		s.logDebug("Preparing:", path)
		lock.Lock()
		err := s.initStageFile(path, part.GetFileSize())
		lock.Unlock()
		s.logDebug("Prepared:", path)
		if err != nil {
			s.logError(err.Error())
		}
	}
}

// Receive reads a single file part with file metadata and reader
func (s *Stage) Receive(file *sts.Partial, reader io.Reader) (err error) {
	if len(file.Parts) != 1 {
		err = fmt.Errorf(
			"can only receive a single part for a single reader (%d given)",
			len(file.Parts))
		return
	}
	part := file.Parts[0]
	path := filepath.Join(s.rootDir, file.Name)

	// Read the part and write it to the right place in the staged "partial"
	fh, err := os.OpenFile(path+partExt, os.O_WRONLY, 0600)
	if err != nil {
		err = fmt.Errorf("failed to open file while trying to write part: %s",
			err.Error())
		return
	}
	if _, err = fh.Seek(part.Beg, 0); err != nil {
		return
	}
	_, err = io.Copy(fh, reader)
	fh.Close()
	if err != nil {
		return
	}

	// Make sure we're the only one updating the companion
	s.logDebug("Receiving part:", file.Source, file.Name, part.Beg, part.End)
	defer s.logDebug("Received part:", file.Source, file.Name, part.Beg, part.End)
	lock := s.getPathLock(path)
	lock.Lock()
	defer lock.Unlock()

	cmp, err := newLocalCompanion(path, file)
	if err != nil {
		return
	}

	if conflict := addCompanionPart(cmp, part.Beg, part.End); conflict != nil {
		s.logInfo(fmt.Sprintf(
			"Removed companion conflict: %s => %d:%d (new) %d:%d (old)",
			cmp.Name, part.Beg, part.End, conflict.Beg, conflict.End))
	}

	if err = writeCompanion(path, cmp); err != nil {
		err = fmt.Errorf("failed to write updated companion: %s", err.Error())
		return
	}

	s.logDebug("Wrote part:", file.Source, file.Name, part.Beg, part.End)

	done := isCompanionComplete(cmp)
	if done {
		final := s.partialToFinal(file)
		existing := s.fromCache(final.path)
		if existing != nil &&
			existing.state != stateFailed &&
			existing.hash == final.hash {
			s.logInfo("Ignoring duplicate (receive):", final.name)
			os.Remove(path + partExt)
			if existing.state >= stateFinalized {
				os.Remove(path + compExt)
				s.delPathLock(path)
			}
			return
		}
		if err = os.Rename(path+partExt, path+fullExt); err != nil {
			err = fmt.Errorf("failed to swap in the \"full\" extension: %s",
				err.Error())
			s.toCache(final, stateFailed)
			return
		}
		s.toCache(final, stateReceived)
		s.logDebug("File received:", cmp.Source, cmp.Name)
		go s.processQueue(final)
	}
	return
}

// Received returns how many (starting at index 0) of the input file parts have
// been successfully received
func (s *Stage) Received(parts []sts.Binned) (n int) {
	for _, part := range parts {
		if !s.partReceived(part) {
			break
		}
		n++
	}
	return
}

func (s *Stage) partReceived(part sts.Binned) bool {
	s.logDebug("Checking for received part:", part.GetName())
	when := part.GetFileTime()
	monthAgo := time.Now().Add(-1 * time.Hour * 24 * 30)
	if when.Before(monthAgo) {
		// Let's put a sensible cap in place
		when = monthAgo
	}
	s.buildCache(when)
	beg, end := part.GetSlice()
	path := filepath.Join(s.rootDir, part.GetName())
	lock := s.getPathLock(path)
	// s.logDebug("Checking for received part:", path)
	// defer s.logDebug("Checked for received part:", path)
	lock.Lock()
	defer lock.Unlock()
	final := &finalFile{
		path:    path,
		renamed: part.GetRenamed(),
		name:    part.GetName(),
		hash:    part.GetFileHash(),
		prev:    part.GetPrev(),
	}
	existing := s.fromCache(final.path)
	if existing == nil {
		if cmp, _ := readLocalCompanion(path, final.name); cmp != nil {
			if final.renamed != cmp.Renamed || final.hash != cmp.Hash || final.prev != cmp.Prev {
				return false
			}
			if companionPartExists(cmp, beg, end) {
				s.logInfo("Part already received:", final.name, beg, end)
				return true
			}
		} else {
			s.delPathLock(path)
		}
	} else if existing.state != stateFailed &&
		existing.hash == final.hash &&
		existing.renamed == final.renamed {
		s.logInfo("File already received:", final.name)
		if existing.state >= stateFinalized {
			s.delPathLock(path)
		}
		return true
	}
	return false
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
	s.logDebug("Stage polled:", sent, relPath)
	s.buildCache(sent)
	path := filepath.Join(s.rootDir, relPath)
	state := s.getFileState(path)
	switch state {
	case stateReceived:
		s.logDebug("Stage:", relPath, "(received)")
		return sts.ConfirmNone
	case stateFailed:
		s.logDebug("Stage:", relPath, "(failed)")
		return sts.ConfirmFailed
	case stateValidated:
		file := s.getWaiting(path)
		if file != nil {
			s.logDebug("Stage:", relPath, "(waiting)")
			return sts.ConfirmWaiting
		}
		s.logDebug("Stage:", relPath, "(done)")
		return sts.ConfirmPassed
	case stateLogged:
		s.logDebug("Stage:", relPath, "(logged)")
		return sts.ConfirmPassed
	case stateFinalized:
		s.logDebug("Stage:", relPath, "(done)")
		return sts.ConfirmPassed
	}
	s.logDebug("Stage:", relPath, "(not found)")
	return sts.ConfirmNone
}

// Ready returns whether or not this gate keeper is ready to receive data
func (s *Stage) Ready() bool {
	s.readyLock.RLock()
	defer s.readyLock.RUnlock()
	return s.canReceive
}

func (s *Stage) setCanReceive(value bool) {
	s.readyLock.Lock()
	defer s.readyLock.Unlock()
	s.canReceive = value
}

// Recover is meant to be run while the server is not so it can cleanly address
// files in the stage area that should be completed from the previous server
// run
func (s *Stage) Recover() {
	s.setCanReceive(false)
	defer s.setCanReceive(true)
	s.logInfo("Beginning stage recovery")
	defer s.logInfo("Stage recovery complete")
	var validate []*sts.Partial
	var finalize []*sts.Partial
	oldest := time.Now()
	err := filepath.Walk(s.rootDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if filepath.Ext(path) != compExt {
				return nil
			}
			cmp, err := readLocalCompanion(path, s.pathToName(path, compExt))
			if err != nil {
				s.logError("Failed to read companion:", path, err.Error())
				return nil
			}
			if info.ModTime().Before(oldest) {
				oldest = info.ModTime()
			}
			base := strings.TrimSuffix(path, compExt)
			if _, err = os.Stat(base + waitExt); !os.IsNotExist(err) {
				// .wait
				s.logDebug("Found ready to finalize:", cmp.Name)
				finalize = append(finalize, cmp)
			} else if _, err = os.Stat(base + fullExt); !os.IsNotExist(err) {
				// .full
				s.logDebug("Found ready to validate:", cmp.Name)
				validate = append(validate, cmp)
			} else if _, err = os.Stat(base + partExt); !os.IsNotExist(err) {
				// .part
				if isCompanionComplete(cmp) {
					if err = os.Rename(base+partExt, base+fullExt); err != nil {
						s.logError("Failed to swap in \"full\" extension:",
							err.Error())
						return nil
					}
					s.logDebug("Found already done:", cmp.Name)
					validate = append(validate, cmp)
				}
			} else if _, err = os.Stat(base); os.IsNotExist(err) {
				// Not found
				if err = os.Remove(path); err != nil {
					s.logError("Failed to remove orphaned companion:",
						path, err.Error())
					return nil
				}
				s.logInfo("Removed orphaned companion:", path)
			} else if isCompanionComplete(cmp) {
				// No extension (backward compatibility)
				if err = os.Rename(base, base+fullExt); err != nil {
					s.logError("Failed to add \"full\" extension:",
						err.Error())
					return nil
				}
				s.logDebug("Found ready to validate:", cmp.Name)
				validate = append(validate, cmp)
			}
			return nil
		})
	if err != nil {
		s.logError(err.Error())
	}
	// Build the cache from the incoming log starting at the time of the oldest
	// companion file found (or "now" if none exists) minus the cache age. Even
	// if no files are found on the stage, we still want to build the cache.
	s.logDebug("Stage recovery cache build:", oldest.Add(-1*cacheAgeLogged))
	s.buildCache(oldest.Add(-1 * cacheAgeLogged))
	if len(validate) == 0 && len(finalize) == 0 {
		return
	}
	for _, file := range finalize {
		finalFile := s.partialToFinal(file)
		s.toCache(finalFile, stateValidated)
		go s.finalizeQueue(finalFile)
	}
	if len(validate) > 0 {
		worker := func(wg *sync.WaitGroup, ch <-chan *sts.Partial) {
			defer wg.Done()
			for f := range ch {
				finalFile := s.partialToFinal(f)
				s.toCache(finalFile, stateReceived)
				s.process(finalFile)
			}
		}
		wg := sync.WaitGroup{}
		ch := make(chan *sts.Partial)
		for i := 0; i < nValidators; i++ {
			wg.Add(1)
			go worker(&wg, ch)
		}
		for _, file := range validate {
			ch <- file
		}
		close(ch)
		wg.Wait()
	}
}

func (s *Stage) CleanNow() {
	s.clean()
}

func (s *Stage) clean() {
	// We only want one cleanup at a time to run
	s.cleanLock.Lock()
	defer s.scheduleClean()
	defer s.cleanLock.Unlock()

	if s.cleanTimeout != nil {
		s.cleanTimeout.Stop()
		s.cleanTimeout = nil
	}

	s.cleanStrays(time.Hour * 24)
	s.cleanWaiting()
}

func (s *Stage) scheduleClean() {
	s.cleanLock.Lock()
	defer s.cleanLock.Unlock()
	if s.cleanTimeout != nil {
		s.cleanTimeout.Stop()
	}
	s.cleanTimeout = time.AfterFunc(s.cleanInterval, func() {
		s.clean()
	})
}

func (s *Stage) Prune(minAge time.Duration) {
	s.pruneTree(s.rootDir, minAge)
	s.pruneTree(s.targetDir, minAge)
}

func (s *Stage) pruneTree(dir string, minAge time.Duration) {
	s.logInfo("Pruning empty directories ...")
	defer s.logInfo("Pruning complete")
	var dirs []string
	err := filepath.Walk(dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil || time.Since(info.ModTime()) < minAge {
				return nil
			}
			if info.IsDir() {
				dirs = append(dirs, path)
			}
			return nil
		})
	if err != nil {
		s.logError(err.Error())
	}
	// Loop backward to process subdirs first (filepath.Walk is sorted)
	for i := len(dirs) - 1; i >= 0; i-- {
		dir := dirs[i]
		entries, err := os.ReadDir(dir)
		if err != nil {
			s.logError("Prune: failed to read directory:", dir, err.Error())
			continue
		}
		if len(entries) == 0 {
			if err = os.Remove(dir); err != nil {
				s.logError("Prune: failed to remove [supposedly empty] directory:", dir, err.Error())
			} else {
				s.logInfo("Prune: removed empty directory:", dir)
			}
		}
	}
}

func (s *Stage) cleanStrays(minAge time.Duration) {
	s.logDebug("Looking for strays ...")
	err := filepath.Walk(s.rootDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			ext := filepath.Ext(path)
			if ext != partExt {
				return nil
			}
			age := time.Since(info.ModTime())
			if age < minAge {
				// Wait a bit before looking for problems
				return nil
			}
			relPath := strings.TrimSuffix(path[len(s.rootDir)+1:], ext)
			compPath := filepath.Join(s.rootDir, relPath+compExt)
			partPath := filepath.Join(s.rootDir, relPath+partExt)
			_, err = os.Stat(compPath)
			compExists := err == nil
			var comp *sts.Partial
			if compExists {
				if comp, err = readLocalCompanion(path, relPath); err != nil {
					s.logError(err.Error())
				}
			}
			s.logDebug("Checking for stray partial:", relPath, compExists, comp != nil)
			delete := false
			deleteCmp := false
			filePath := strings.TrimSuffix(path, partExt)
			fileState := s.getFileState(filePath)
			fileHash := s.getFileHash(filePath)
			if fileState > stateReceived {
				delete = comp == nil || comp.Hash == fileHash
				deleteCmp = compExists && fileState == stateLogged
				s.logDebug("Stray partial cache info:", relPath, fileState, fileHash)
			} else {
				end := time.Now()
				beg := info.ModTime().Add(time.Duration(age.Minutes()) * time.Hour * -1)
				s.logInfo("Checking log for stray partial:", relPath, "--", beg.Format("20060102"))
				hash := ""
				if comp != nil {
					hash = comp.Hash
				}
				if s.logger.WasReceived(relPath, hash, beg, end) {
					delete = true
					deleteCmp = compExists
				}
			}
			if delete {
				if err = os.Remove(partPath); err != nil {
					s.logError("Failed to remove stray partial:", path, err.Error())
					return nil
				}
				s.logInfo("Deleted stray partial:", relPath)
			}
			if deleteCmp {
				if err = os.Remove(compPath); err != nil {
					s.logError("Failed to remove stray partial companion:", compPath, err.Error())
					return nil
				}
				s.logInfo("Deleted stray partial companion:", compPath)
			}
			return nil
		})
	if err != nil {
		s.logError(err.Error())
	}
}

func (s *Stage) cleanWaiting() {
	s.logDebug("Looking for wait loops ...")
	var waiting []*finalFile
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	for _, cacheFile := range s.cache {
		if cacheFile.state != stateValidated ||
			cacheFile.prev == "" {
			continue
		}
		waiting = append(waiting, cacheFile)
	}
	sort.Slice(waiting, func(i, j int) bool {
		return waiting[i].time.Before(waiting[j].time)
	})
	for _, cacheFile := range waiting {
		prevPath := filepath.Join(s.rootDir, cacheFile.prev)
		if !s.isWaiting(prevPath) {
			continue
		}
		s.logDebug("Checking for wait loop:", cacheFile.name, "<-", cacheFile.prev)
		loop := s.detectWaitLoop(prevPath)
		if len(loop) == 0 {
			continue
		}
		for _, waitFile := range s.fromWait(prevPath) {
			s.logInfo("Removing wait loop:", waitFile.name, "<-", waitFile.prev)
			f := s.fromCache(waitFile.path)
			if f != nil && f.state == stateValidated {
				if f.wait != nil {
					f.wait.Stop()
					f.wait = nil
				}
				f.prev = ""
				go s.finalizeQueue(f)
			}
		}
	}
}

// Stop waits for the number of files in the pipe to be zero and then signals
func (s *Stage) Stop(force bool) {
	s.logDebug("Stage shutting down ...")
	defer s.logDebug("Stage shut down")
	// This seems like a good idea, but there's not a good way to know nothing is still
	// "active" and it seems too risky without that knowledge.
	// defer s.clearCache()
	defer s.setCanReceive(false)
	if force {
		return
	}
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
		path:    filepath.Join(s.rootDir, file.Name),
		name:    file.Name,
		renamed: file.Renamed,
		size:    file.Size,
		hash:    file.Hash,
		prev:    file.Prev,
	}
}

func (s *Stage) processQueue(file *finalFile) {
	// s.logDebug("Pushing onto validate chan:", file.name)
	// defer s.logDebug("Pushed onto validate chan:", file.name)
	s.validateCh <- file
}

func (s *Stage) processHandler() {
	for f := range s.validateCh {
		s.process(f)
	}
}

func (s *Stage) process(file *finalFile) {
	// s.logDebug("Validating:", file.name)
	// defer s.logDebug("Validated:", file.name)

	fileLock := s.getPathLock(file.path)
	fileLock.Lock()
	defer fileLock.Unlock()

	existingState := s.getFileState(file.path)
	if existingState == stateUnknown || existingState != stateReceived {
		s.logDebug("Ignoring invalid (process):", file.name)
		return
	}

	// Validate checksum.
	hash, err := fileutil.FileMD5(file.path + fullExt)
	if err != nil {
		os.Remove(file.path + compExt)
		os.Remove(file.path + fullExt)
		s.logError(fmt.Sprintf(
			"Failed to calculate MD5 of %s: %s",
			file.name, err.Error()))
		s.toCache(file, stateFailed)
		return
	}

	valid := file.hash == hash

	if !valid {
		s.logError(fmt.Sprintf("Failed validation: %s (%s => %s)",
			file.path, file.hash, hash))
		s.toCache(file, stateFailed)
		return
	}

	if err = os.Rename(file.path+fullExt, file.path+waitExt); err != nil {
		s.logError(fmt.Sprintf(
			"Failed to swap in \"wait\" extension: %s",
			err.Error()))
		s.toCache(file, stateFailed)
		return
	}

	s.toCache(file, stateValidated)

	go s.finalizeQueue(file)
}

func (s *Stage) finalizeQueue(file *finalFile) {
	s.logDebug("Pushing onto finalize chan:", file.name)
	defer s.logDebug("Pushed onto finalize chan:", file.name)
	s.finalizeCh <- file
}

func (s *Stage) finalizeHandler() {
	defer s.logDebug("Finalize channel done:")
	for f := range s.finalizeCh {
		s.logDebug("Finalize chain:", f.name)
		if state := s.getFileState(f.path); state != stateValidated {
			// Skip redundancies or mistakes in the pipe
			s.logDebug("Already finalized or not ready:", f.name)
			continue
		}
		if s.isFileReady(f) {
			s.finalize(f)
		}
	}
}

func (s *Stage) isFileReady(file *finalFile) bool {
	if file.prev == "" || file.prev == file.name {
		return true
	}
	var waitTime time.Duration
	prevPath := filepath.Join(s.rootDir, file.prev)
	prevState := s.getFileState(prevPath)
	switch prevState {
	case stateUnknown:
		if s.hasPathLock(prevPath) {
			s.logDebug("Previous file in progress:", file.name, "<-", file.prev)
			break
		}
		file.prevScan = time.Now()
		age := time.Since(file.time)
		len := time.Duration(age.Minutes()) * time.Hour * 24
		if len == 0 {
			len = time.Hour * 24
		}
		end := file.prevScanBeg
		if end.IsZero() || end.Year() < 2010 {
			// Start over if we end up farther back than something
			// reasonable; 2010 predates this program, so that's a
			// pretty safe bet
			end = s.getCacheStartTime()
		}
		beg := end.Add(-1 * len)
		// ...then check the log increasingly farther back each time we
		// get here to try to find this file's predecessor
		file.prevScanBeg = beg
		t := time.Now()
		found := s.logger.WasReceived(file.prev, "", beg, end)
		took := fmt.Sprintf("(took %s)", time.Since(t))
		tfmt := "20060102"
		if found {
			s.logInfo(
				"Found previous in log:",
				file.name, "<-", file.prev, "--", beg.Format(tfmt), "-", end.Format(tfmt), took)
			return true
		}
		s.logInfo("Previous file not found in log:",
			file.name, "<-", file.prev, "--", beg.Format(tfmt), "-", end.Format(tfmt), took)
		waitTime = time.Second * 10

	case stateReceived:
		s.logDebug("Waiting for previous file:",
			s.name, file.name, "<-", file.prev)

	case stateFailed:
		s.logDebug("Previous file failed:",
			s.name, file.name, "<-", file.prev)

	case stateValidated:
		if s.isWaiting(prevPath) {
			s.logDebug("Previous file waiting:", file.name, "<-", file.prev)
		} else {
			s.logDebug("Previous file validated:", file.name, "<-", file.prev)
		}

	default:
		return true
	}

	s.toWait(prevPath, file, waitTime)
	return false
}

func (s *Stage) finalize(file *finalFile) {
	fileLock := s.getPathLock(file.path)
	// s.logDebug("Finalizing prep", file.name)
	fileLock.Lock()
	defer s.delPathLock(file.path)
	defer fileLock.Unlock()

	existingState := s.getFileState(file.path)
	if existingState != stateValidated {
		s.logDebug("Ignoring invalid (final):", file.name, existingState)
		return
	}

	if file.wait != nil {
		file.wait.Stop()
		file.wait = nil
	}

	s.logDebug("Finalizing", file.name)
	targetPath, err := s.putFileAway(file)
	if err != nil {
		s.logError(err)
		return
	}
	s.logDebug("Finalized", file.name)

	if s.exporter != nil {
		s.logDebug("Exporting", targetPath)
		if err := s.exporter.Upload(targetPath, file.name); err != nil {
			s.logError(err)
		} else {
			s.logDebug("Exported", targetPath)
			os.Remove(targetPath)
			s.logDebug("Removed exported file:", targetPath)
		}
	}

	if s.dispatcher != nil {
		s.logDebug("Dispatching", targetPath)
		if err := s.dispatcher.Send(targetPath); err != nil {
			s.logError(err)
		}
	}

	waiting := s.fromWait(file.path)
	for _, waitFile := range waiting {
		s.logDebug("Stage found waiting:", waitFile.name, "<-", file.name)
		go s.finalizeQueue(waitFile)
	}
}

func (s *Stage) putFileAway(file *finalFile) (targetPath string, err error) {
	s.logDebug("Putting file away:", file.name)
	defer s.logDebug("Put away:", file.name)

	// Better to log the file twice rather than receive it twice.  If we log
	// after putting the file away, it's possible that a crash could occur
	// after putting the file away but before logging. On restart, there would
	// be no knowledge that the file was received and it would be sent again.
	s.logger.Received(file)
	file.logged = time.Now()

	// Move it
	targetName := file.name
	if file.renamed != "" {
		targetName = file.renamed
	}
	targetPath = filepath.Join(s.targetDir, targetName)
	if err = os.MkdirAll(filepath.Dir(targetPath), 0775); err != nil {
		return
	}
	if err = fileutil.Move(file.path+waitExt, targetPath); err != nil {
		// If the file doesn't exist then something is really wrong.
		// Either we somehow have two instances running that are stepping
		// on each other or we have an illusive and critical bug.  Regardless,
		// it's not good.
		if !os.IsNotExist(err) {
			// If the file is there but we still got an error, let's just try
			// it again later.
			file.nErr++
			time.AfterFunc(time.Second*time.Duration(file.nErr), func() {
				s.logDebug("Attempting finalize again after failure:", file.name)
				go s.finalizeQueue(file)
			})
		}
		err = fmt.Errorf(
			"failed to move %s to %s: %s",
			file.path+waitExt, targetPath, err.Error())
		return
	}

	// Only change the state once the file has been successfully moved
	s.toCache(file, stateFinalized)

	// Clean up the companion (no need to capture an error since it wouldn't
	// be a deal-breaker anyway)
	os.Remove(file.path + compExt)
	return
}

func (s *Stage) isWaiting(path string) bool {
	return s.getWaiting(path) != nil
}

// delectWaitLoop recurses through the wait tree to see if there is a file that
// points back to the original (thus indicating a loop) and returns the names of
// the files in the loop
func (s *Stage) detectWaitLoop(prevPath string) (loop map[string]string) {
	s.waitLock.RLock()
	defer s.waitLock.RUnlock()
	paths := []string{prevPath}
	var next []string
	loop = make(map[string]string)
	for {
		for _, p := range paths {
			ff, ok := s.wait[p]
			if !ok {
				continue
			}
			for _, f := range ff {
				if f.path == prevPath {
					if len(loop) == 0 {
						// If the file ends up waiting on itself for some reason
						loop[f.path] = p
					}
					s.logInfo("Wait loop detected:", p, "<-", prevPath, "--", len(loop))
					return
				}
				if _, ok := loop[f.path]; ok {
					continue
				}
				loop[f.path] = p
				next = append(next, f.path)
			}
		}
		if next == nil {
			break
		}
		paths = next
		next = nil
	}
	loop = nil
	return
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

// fromWait returns the file(s) currently waiting on the file indicated by path
func (s *Stage) fromWait(prevPath string) []*finalFile {
	// s.logDebug("Wait reading", prevPath)
	// defer s.logDebug("Wait read", prevPath)
	s.waitLock.Lock()
	defer s.waitLock.Unlock()
	files, ok := s.wait[prevPath]
	if ok {
		delete(s.wait, prevPath)
		s.logDebug("Waiting:", len(s.wait))
		return files
	}
	return nil
}

func (s *Stage) toWait(prevPath string, next *finalFile, howLong time.Duration) {
	// s.logDebug("Wait updating", prevPath, "->", next.path)
	// defer s.logDebug("Wait updated", prevPath, "->", next.path)
	s.waitLock.Lock()
	defer s.waitLock.Unlock()
	if next.wait != nil {
		next.wait.Stop()
		next.wait = nil
	}
	if howLong > 0 {
		// Set a timer to check this file again in case there is a wait loop or in
		// case the predecessor was received so long ago we have to dig through the
		// logs to find it
		next.wait = time.AfterFunc(howLong, func(handle func(*finalFile), f *finalFile) func() {
			return func() {
				s.logDebug("Attempting finalize again:", f.name)
				handle(f)
			}
		}(s.finalizeQueue, next))
	}
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
	// s.logDebug("Cache reading", path)
	// defer s.logDebug("Cache read", path)
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	return s.cache[path]
}

func (s *Stage) getFileState(path string) int {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	if f, ok := s.cache[path]; ok {
		return f.state
	}
	return stateUnknown
}

func (s *Stage) getFileHash(path string) string {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	if f, ok := s.cache[path]; ok {
		return f.hash
	}
	return ""
}

func (s *Stage) inPipe() int {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	return s.nPipe
}

func (s *Stage) toCache(file *finalFile, state int) {
	// s.logDebug("Caching", file.path, state)
	// defer s.logDebug("Cached", file.path, state)
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
	if file.prev != "" && file.state == stateFinalized {
		prevPath := filepath.Join(s.rootDir, file.prev)
		if prev, ok := s.cache[prevPath]; ok {
			prev.nextFinal = true
		}
	}
	if len(s.cache)%cacheCnt == 0 {
		go s.cleanCache()
	}
}

func (s *Stage) buildCache(from time.Time) {
	if from.IsZero() {
		return
	}
	if func(s *Stage, t1 time.Time) bool {
		s.cacheLock.RLock()
		defer s.cacheLock.RUnlock()
		s.logDebug("Cache build request:", s.cacheTime, t1)
		return !s.cacheTime.IsZero() && (s.cacheTime.Before(t1) || s.cacheTime.Equal(t1))
	}(s, from) {
		return
	}
	// s.logDebug("Building cache from logs:", from)
	// defer s.logDebug("Built cache from logs:", from)
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	cacheTime := s.cacheTime
	if cacheTime.IsZero() {
		cacheTime = time.Now()
	}
	s.logInfo("Building cache from logs:", from)
	var first time.Time
	now := time.Now()
	s.logger.Parse(func(name, renamed, hash string, size int64, t time.Time) bool {
		if t.After(cacheTime) {
			return true
		}
		if first.IsZero() {
			first = t
		}
		path := filepath.Join(s.rootDir, name)
		if _, ok := s.cache[path]; ok {
			// Skip it if the file is already in the cache
			return false
		}
		file := &finalFile{
			path:    path,
			name:    name,
			renamed: renamed,
			hash:    hash,
			size:    size,
			time:    now,
			logged:  t,
			state:   stateLogged,
		}
		s.cache[path] = file
		s.logDebug("Cached from log:", file.name)
		return false
	}, from, cacheTime)
	if !first.IsZero() {
		s.cacheTimes = append(s.cacheTimes, now)
	}
	s.cacheTime = from
}

func (s *Stage) getCacheStartTime() time.Time {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()
	return s.cacheTime
}

// func (s *Stage) clearCache() {
// 	s.cacheLock.Lock()
// 	defer s.cacheLock.Unlock()
// 	s.cache = make(map[string]finalFile)
// 	s.cacheTimes = nil
// 	s.cacheTime = time.Time{}
// }

func (s *Stage) cleanCache() {
	// s.logDebug("Cleaning cache")
	// defer s.logDebug("Cleaned cache")
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	var age time.Duration
	var batches []time.Time
	if len(s.cacheTimes) > 0 {
		for _, t := range s.cacheTimes {
			if time.Since(t) < cacheAgeLoaded {
				// We want to expire batches in order so that we never
				// leave a gap in the cache
				break
			}
			batches = append(batches, t)
		}
		s.cacheTimes = s.cacheTimes[len(batches):]
	}
	s.cacheTime = time.Now()
	for _, cacheFile := range s.cache {
		if cacheFile.state < stateFinalized {
			continue
		}
		if cacheFile.prev != "" && !cacheFile.nextFinal {
			continue
		}
		age = time.Since(cacheFile.logged)
		if age > cacheAgeLogged {
			if len(batches) > 0 {
				for _, t := range batches {
					if t.Equal(cacheFile.time) && cacheFile.state == stateLogged {
						// Delete if file was loaded via a batch that is
						// ready to be expired
						goto delete
					}
				}
				goto keep
			}
		delete:
			delete(s.cache, cacheFile.path)
			s.logDebug("Removed from cache:", cacheFile.name)
			continue
		}
	keep:
		// We want the source cacheTime to be the earliest logged time of
		// the files still in the cache (except the extra ones we keep
		// around to maintain the chain)
		if cacheFile.logged.Before(s.cacheTime) {
			s.cacheTime = cacheFile.logged
		}
	}
	s.logDebug("Cache Count:", len(s.cache))
	s.logDebug("Cache Batch Count:", len(s.cacheTimes))
}

func (s *Stage) formatLogPrefix() string {
	return fmt.Sprintf("(%s)", s.name)
}

func (s *Stage) logDebug(params ...interface{}) {
	log.Debug(append([]any{s.formatLogPrefix()}, params...)...)
}

func (s *Stage) logInfo(params ...interface{}) {
	log.Info(append([]any{s.formatLogPrefix()}, params...)...)
}

func (s *Stage) logError(params ...interface{}) {
	log.Error(append([]any{s.formatLogPrefix()}, params...)...)
}
