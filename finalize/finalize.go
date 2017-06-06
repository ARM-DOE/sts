package finalize

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/companion"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/logging"
	"code.arm.gov/dataflow/sts/poll"
	"code.arm.gov/dataflow/sts/server"
)

// CacheAge is how long a finalized file is kept in memory.
const CacheAge = time.Minute * 60

// CacheCnt is the number of files after which to age the cache.
const CacheCnt = 1000

// WaitPollCount is the number of times a file can be polled before a file
// in waiting is released without any sign of its predecessor.
const WaitPollCount = 5

// LogSearch is how far back (max) to look in the logs for a finalized file.
const LogSearch = time.Duration(30 * 24 * time.Hour)

// Finalizer manages files that need to be validated against companions.
type Finalizer struct {
	Conf      *server.ReceiverConf
	last      time.Time
	poll      map[string]int
	pollLock  sync.RWMutex
	wait      map[string][]*finalFile
	waitLock  sync.RWMutex
	cache     map[string]*finalFile
	cacheLock sync.RWMutex
	validChan chan *finalFile
}

type finalFile struct {
	file    sts.ScanFile
	comp    *companion.Companion
	hash    string
	time    time.Time
	success bool
}

// NewFinalizer returns a new Finalizer instance.
func NewFinalizer(conf *server.ReceiverConf) *Finalizer {
	f := &Finalizer{}
	f.Conf = conf
	f.poll = make(map[string]int)
	f.wait = make(map[string][]*finalFile)
	f.cache = make(map[string]*finalFile)
	return f
}

// Start starts the finalizer daemon that listens on the provided channel.
func (f *Finalizer) Start(inChan chan []sts.ScanFile) {
	f.validChan = make(chan *finalFile, cap(inChan))
	for {
		select {
		case files, ok := <-inChan:
			if !ok {
				return
			}
			for _, file := range files {
				go f.validate(file, f.validChan)
			}
		case ff, ok := <-f.validChan:
			if !ok {
				return
			}
			if _, found := f.fromCache(ff.file.GetPath(false)); found {
				// It's possible to get duplicates on the validChan since the
				// externally visible GetFileStatus() can put files here that
				// are also in the wait cache.  We just need to let the first
				// one win and the cache is a good way to do that.
				continue
			}
			done, err := f.finalize(ff)
			if err != nil {
				logging.Error(err.Error())
				continue
			}
			if done {
				// See if any files were waiting on this one.
				n := f.fromWait(ff.file.GetPath(false))
				if n != nil {
					for _, wf := range n {
						logging.Debug("FINAL Found Waiting:", wf.file.GetRelPath())
						go func(wf *finalFile, out chan *finalFile) {
							out <- wf
						}(wf, f.validChan)
					}
				}
			}
		}
	}
}

// GetFileStatus is for outside components to poll the confirmation status of a file.
// It returns one of these constants:
// -> ConfirmPassed: MD5 validation was successful and file put away
// -> ConfirmFailed: MD5 validation was unsuccessful and file should be resent
// -> ConfirmWaiting: MD5 validation was successful but waiting on predecessor
// -> ConfirmNone: No knowledge of file
func (f *Finalizer) GetFileStatus(source, relPath string, sent time.Time) int {
	path := filepath.Join(f.Conf.StageDir, source, relPath)
	success, found := f.fromCache(path)
	if success {
		f.removePoll(path)
		return poll.ConfirmPassed
	}
	if !found {
		wf := f.getWaiting(path)
		if wf != nil {
			if f.validChan != nil {
				go func(wf *finalFile, out chan *finalFile) {
					out <- wf
				}(wf, f.validChan)
			}
			f.incrementPollCount(path)
			return poll.ConfirmWaiting
		}
		if logging.FindReceived(relPath, sent, time.Now(), source) {
			f.removePoll(path)
			return poll.ConfirmPassed
		}
		return poll.ConfirmNone
	}
	return poll.ConfirmFailed
}

func (f *Finalizer) validate(file sts.ScanFile, out chan<- *finalFile) {
	var err error

	// Make sure it exists.
	if _, err = os.Stat(file.GetPath(false)); err != nil {
		return
	}

	logging.Debug("FINAL Validating:", file.GetRelPath())

	ff := &finalFile{file: file}

	// Read companion metadata.
	if _, ok := file.(sts.RecvFile); ok {
		ff.comp = file.(sts.RecvFile).GetCompanion().(*companion.Companion)
	} else {
		if ff.comp, err = companion.ReadCompanion(file.GetPath(false)); err != nil {
			os.Remove(file.GetPath(false))
			logging.Error(fmt.Sprintf("Missing/invalid companion (%s): %s", err.Error(), file.GetRelPath()))
			return
		}
	}

	// Validate checksum.
	if ff.hash, err = fileutil.FileMD5(file.GetPath(false)); err != nil {
		ff.comp.Delete()
		os.Remove(file.GetPath(false))
		logging.Error(fmt.Sprintf("Failed to calculate MD5 of %s: %s", file.GetRelPath(), err.Error()))
		return
	}
	if ff.hash != ff.comp.Hash {
		f.toCache(ff)
		// ff.comp.Delete()
		// os.Remove(file.GetPath(false))
		logging.Error(fmt.Sprintf("Failed validation: %s", file.GetPath(false)))
		return
	}

	logging.Debug("FINAL Validated:", file.GetRelPath())

	// Send to be finalized.
	ff.success = true
	out <- ff
}

func (f *Finalizer) finalize(ff *finalFile) (done bool, err error) {

	// Make sure it doesn't need to wait in line.
	if ff.comp.PrevFile != "" {
		stagedPrevFile := filepath.Join(f.Conf.StageDir, ff.comp.SourceName, ff.comp.PrevFile)
		success, found := f.fromCache(stagedPrevFile)
		if !found {
			if f.isWaiting(stagedPrevFile) {
				logging.Debug("FINAL Previous File Waiting:", ff.file.GetRelPath(), "<-", ff.comp.PrevFile)
				f.toWait(stagedPrevFile, ff)
				return
			}
			if _, err = os.Stat(stagedPrevFile + server.PartExt); !os.IsNotExist(err) {
				logging.Debug("FINAL Previous File in Progress:", ff.file.GetRelPath(), "<-", ff.comp.PrevFile)
				f.toWait(stagedPrevFile, ff)
				err = nil
				return
			}
			if _, err = os.Stat(stagedPrevFile); !os.IsNotExist(err) {
				logging.Debug("FINAL Waiting for Previous File:", ff.file.GetRelPath(), "<-", ff.comp.PrevFile)
				f.toWait(stagedPrevFile, ff)
				err = nil
				return
			}
			if !logging.FindReceived(ff.comp.PrevFile, time.Now(), time.Now().Add(-LogSearch), ff.comp.SourceName) {
				logging.Debug("FINAL Previous File Not Found in Log:", ff.file.GetRelPath(), "<-", ff.comp.PrevFile)
				if count := f.getPollCount(ff.file.GetPath(false)); count < WaitPollCount {
					f.toWait(stagedPrevFile, ff)
					err = nil
					return
				}
				// If this file has been polled MaxPollCount times and we still can't
				// find any trace of its predecessor (which is the case if we get here)
				// then release it anyway at the risk of getting data out of order.
				// We can only do so much.
				logging.Info("Done Waiting:", ff.file.GetRelPath(), "<-", ff.comp.PrevFile)
			}
		} else if !success {
			logging.Debug("FINAL Previous File Failed:", ff.file.GetRelPath(), "<-", ff.comp.PrevFile)
			f.toWait(stagedPrevFile, ff)
			return
		}
	}

	logging.Debug("FINAL Finalizing", ff.comp.SourceName, ff.file.GetRelPath())

	// Let's log it and update the cache before actually putting the file away in order
	// to properly recover in case something happens between now and then.
	logging.Received(ff.file.GetRelPath(), ff.comp.Hash, ff.comp.Size, ff.comp.SourceName)

	f.toCache(ff)

	// Move it.
	finalPath := filepath.Join(f.Conf.FinalDir, ff.comp.SourceName, ff.file.GetRelPath())
	os.MkdirAll(filepath.Dir(finalPath), os.ModePerm)
	if err = os.Rename(ff.file.GetPath(false), finalPath+fileutil.LockExt); err != nil {
		err = fmt.Errorf("Failed to move %s to %s: %s", ff.file.GetPath(false), finalPath+fileutil.LockExt, err.Error())
		return
	}
	if err = os.Rename(finalPath+fileutil.LockExt, finalPath); err != nil {
		err = fmt.Errorf("Failed to unlock %s: %s", finalPath, err.Error())
		return
	}

	// Clean up the companion.
	if err = ff.comp.Delete(); err != nil {
		err = fmt.Errorf("Failed to remove companion file for %s: %s", ff.comp.Path, err.Error())
		return
	}

	logging.Debug("FINAL Finalized", ff.comp.SourceName, ff.file.GetRelPath())

	done = true
	return
}

func (f *Finalizer) getPollCount(path string) int {
	f.pollLock.RLock()
	defer f.pollLock.RUnlock()
	if count, ok := f.poll[path]; ok {
		return count
	}
	return 0
}

func (f *Finalizer) incrementPollCount(path string) (c int) {
	f.pollLock.Lock()
	defer f.pollLock.Unlock()
	var ok bool
	if c, ok = f.poll[path]; ok {
		c++
		f.poll[path] = c
		return
	}
	c = 1
	f.poll[path] = c
	return
}

func (f *Finalizer) removePoll(path string) {
	f.pollLock.Lock()
	defer f.pollLock.Unlock()
	if _, ok := f.poll[path]; ok {
		delete(f.poll, path)
	}
}

func (f *Finalizer) isWaiting(path string) bool {
	return f.getWaiting(path) != nil
}

func (f *Finalizer) getWaiting(path string) *finalFile {
	f.waitLock.RLock()
	defer f.waitLock.RUnlock()
	for _, fList := range f.wait {
		for _, ff := range fList {
			if ff.file.GetPath(false) == path {
				return ff
			}
		}
	}
	return nil
}

func (f *Finalizer) fromWait(path string) []*finalFile {
	f.waitLock.Lock()
	defer f.waitLock.Unlock()
	fList, ok := f.wait[path]
	if ok {
		delete(f.wait, path)
		return fList
	}
	return nil
}

func (f *Finalizer) toWait(path string, ff *finalFile) {
	f.waitLock.Lock()
	defer f.waitLock.Unlock()
	files, ok := f.wait[path]
	if ok {
		for _, wf := range files {
			if wf.file.GetPath(false) == ff.file.GetPath(false) {
				return
			}
		}
		files = append(files, ff)
	} else {
		files = []*finalFile{ff}
	}
	f.wait[path] = files
}

func (f *Finalizer) fromCache(path string) (bool, bool) {
	f.cacheLock.RLock()
	defer f.cacheLock.RUnlock()
	file, exists := f.cache[path]
	if exists {
		return file.success, true
	}
	return false, false
}

func (f *Finalizer) toCache(ff *finalFile) {
	f.cacheLock.Lock()
	defer f.cacheLock.Unlock()
	now := time.Now()
	ff.time = now
	f.cache[ff.file.GetPath(false)] = ff
	if ff.success {
		f.last = now
	}
	if len(f.cache)%CacheCnt == 0 {
		for key, cfile := range f.cache {
			if now.Sub(cfile.time) > CacheAge {
				logging.Debug("FINAL Clean Cache:", key)
				delete(f.cache, key)
			}
		}
	}
}
