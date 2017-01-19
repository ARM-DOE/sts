package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/logging"
)

// CacheAge is how long a finalized file is kept in memory.
const CacheAge = time.Minute * 60

// CacheCnt is the number of files after which to age the cache.
const CacheCnt = 1000

// LogSearch is how far back (max) to look in the logs for a finalized file.
const LogSearch = time.Duration(30 * 24 * time.Hour)

// Finalizer manages files that need to be validated against companions.
type Finalizer struct {
	Conf      *ReceiverConf
	last      time.Time
	wait      map[string][]ScanFile
	waitLock  sync.RWMutex
	cache     map[string]*finalFile
	cacheLock sync.RWMutex
}

type finalFile struct {
	file    ScanFile
	time    time.Time
	success bool
}

// NewFinalizer returns a new Finalizer instance.
func NewFinalizer(conf *ReceiverConf) *Finalizer {
	f := &Finalizer{}
	f.Conf = conf
	f.wait = make(map[string][]ScanFile)
	f.cache = make(map[string]*finalFile)
	return f
}

// Start starts the finalizer daemon that listens on the provided channel.
func (f *Finalizer) Start(inChan chan []ScanFile) {
	for {
		files, ok := <-inChan
		if !ok {
			break
		}
		// Let's keep things moving by running each batch asynchronously.
		go f.finalizeBatch(files)
	}
}

// GetFileStatus is for outside components to ask for the confirmation status of a file.
// It returns one of these constants: ConfirmPassed, ConfirmFailed, ConfirmNone
func (f *Finalizer) GetFileStatus(source string, relPath string, sent time.Time) int {
	path := filepath.Join(f.Conf.StageDir, source, relPath)
	success, found := f.fromCache(path)
	if success {
		return ConfirmPassed
	}
	if !found {
		if f.isWaiting(path) {
			return ConfirmNone
		}
		if logging.FindReceived(relPath, sent, time.Now(), source) {
			return ConfirmPassed
		}
		return ConfirmNone
	}
	return ConfirmFailed
}

func (f *Finalizer) finalizeBatch(files []ScanFile) {
	var next []ScanFile
	for {
		if len(files) == 0 {
			break
		}
		next = nil
		for _, file := range files {
			valid, err := f.finalize(file)
			if err != nil {
				logging.Error(err.Error())
			}
			if valid {
				// See if any files were waiting on this one.
				n := f.fromWait(file.GetPath(false))
				if n != nil {
					logging.Debug("FINAL Found Waiting: ", n[0].GetRelPath(), len(n))
					next = append(next, n...)
				}
			}
		}
		files = next
	}
}

func (f *Finalizer) finalize(file ScanFile) (valid bool, err error) {
	// Make sure it exists.
	info, err := os.Stat(file.GetPath(false))
	if err != nil {
		return
	}

	// Read companion metadata.
	var cmp *Companion
	if _, ok := file.(RecvFile); ok {
		cmp = file.(RecvFile).GetCompanion()
	} else {
		cmp, err = ReadCompanion(file.GetPath(false))
		if err != nil {
			os.Remove(file.GetPath(false))
			return false, fmt.Errorf("Missing/invalid companion (%s): %s", err.Error(), file.GetRelPath())
		}
	}

	// Make sure it doesn't need to wait in line.
	if cmp.PrevFile != "" {
		stagedPrevFile := filepath.Join(f.Conf.StageDir, cmp.SourceName, cmp.PrevFile)
		success, found := f.fromCache(stagedPrevFile)
		if !found {
			_, err = os.Stat(stagedPrevFile + PartExt)
			if !os.IsNotExist(err) {
				logging.Debug("FINAL Previous File in Progress:", file.GetRelPath(), "<-", cmp.PrevFile)
				f.toWait(stagedPrevFile, file)
				return false, nil
			}
			_, err = os.Stat(stagedPrevFile)
			if !os.IsNotExist(err) {
				logging.Debug("FINAL Waiting for Previous File:", file.GetRelPath(), "<-", cmp.PrevFile)
				f.toWait(stagedPrevFile, file)
				return false, nil
			}
			found = logging.FindReceived(cmp.PrevFile, time.Now(), time.Now().Add(-LogSearch), cmp.SourceName)
			if !found {
				logging.Debug("FINAL Previous File Not Found in Log:", file.GetRelPath(), "<-", cmp.PrevFile)
				f.toWait(stagedPrevFile, file)
				return false, nil
			}
		} else if !success {
			logging.Debug("FINAL Previous File Failed:", file.GetRelPath(), "<-", cmp.PrevFile)
			f.toWait(stagedPrevFile, file)
			return false, nil
		}
	}

	// Validate checksum.
	hash, err := fileutils.FileMD5(file.GetPath(false))
	if err != nil {
		return false, fmt.Errorf("Failed to calculate MD5 of %s: %s", file.GetRelPath(), err.Error())
	}
	if hash != cmp.Hash {
		f.toCache(file, false)
		cmp.Delete()
		os.Remove(file.GetPath(false))
		return false, fmt.Errorf("Failed validation: %s", file.GetPath(false))
	}

	logging.Debug("FINAL Finalizing", cmp.SourceName, file.GetRelPath())

	// Let's log it and update the cache before actually putting the file away in order
	// to properly recover in case something happens between now and then.
	logging.Received(file.GetRelPath(), hash, info.Size(), cmp.SourceName)

	f.toCache(file, true)

	// Move it.
	finalPath := filepath.Join(f.Conf.FinalDir, cmp.SourceName, file.GetRelPath())
	os.MkdirAll(filepath.Dir(finalPath), os.ModePerm)
	if err = os.Rename(file.GetPath(false), finalPath+fileutils.LockExt); err != nil {
		return false, fmt.Errorf("Failed to move %s to %s: %s", file.GetPath(false), finalPath+fileutils.LockExt, err.Error())
	}
	if err = os.Rename(finalPath+fileutils.LockExt, finalPath); err != nil {
		return false, fmt.Errorf("Failed to unlock %s: %s", finalPath, err.Error())
	}

	// Clean up the companion.
	if err = cmp.Delete(); err != nil {
		return true, fmt.Errorf("Failed to remove companion file for %s: %s", cmp.Path, err.Error())
	}

	logging.Debug("FINAL Finalized", cmp.SourceName, file.GetRelPath())

	valid = true
	return
}

func (f *Finalizer) isWaiting(path string) bool {
	f.waitLock.RLock()
	defer f.waitLock.RUnlock()
	for _, ff := range f.wait {
		for _, file := range ff {
			if file.GetPath(false) == path {
				return true
			}
		}
	}
	return false
}

func (f *Finalizer) fromWait(path string) []ScanFile {
	f.waitLock.Lock()
	defer f.waitLock.Unlock()
	files, ok := f.wait[path]
	if ok {
		delete(f.wait, path)
		return files
	}
	return nil
}

func (f *Finalizer) toWait(path string, file ScanFile) {
	f.waitLock.Lock()
	defer f.waitLock.Unlock()
	files, ok := f.wait[path]
	if ok {
		for _, ff := range files {
			if ff.GetPath(false) == file.GetPath(false) {
				return
			}
		}
		files = append(files, file)
	} else {
		files = []ScanFile{file}
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

func (f *Finalizer) toCache(file ScanFile, success bool) {
	f.cacheLock.Lock()
	defer f.cacheLock.Unlock()
	now := time.Now()
	ff := &finalFile{file: file, time: now, success: success}
	f.cache[file.GetPath(false)] = ff
	if success {
		f.last = ff.time
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
