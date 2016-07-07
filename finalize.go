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
	f.cache = make(map[string]*finalFile)
	return f
}

// Start starts the finalizer daemon that listens on the provided channel.
func (f *Finalizer) Start(inChan chan []ScanFile) {
	for {
		files, ok := <-inChan
		if !ok {
			return
		}
		for _, file := range files {
			_, err := f.finalize(file)
			if err != nil {
				logging.Error(err.Error())
			}
		}
	}
}

// IsFinal is for outside components to ask if the specified file has been finalized.
func (f *Finalizer) IsFinal(source string, relPath string, sent time.Time) (bool, bool) {
	success, found := f.fromCache(source, relPath)
	if !found {
		success = logging.FindReceived(relPath, sent, time.Now(), source)
		return success, success
	}
	return success, true
}

func (f *Finalizer) finalize(file ScanFile) (bool, error) {
	// Check it.
	info, err := os.Stat(file.GetPath(false))
	if os.IsNotExist(err) {
		return false, nil // Was probably just picked up by the scanner again before getting moved.
	} else if err != nil {
		return false, fmt.Errorf(err.Error())
	}

	// Read about it.
	cmp, err := ReadCompanion(file.GetPath(true))
	if err != nil {
		os.Remove(file.GetPath(false))
		return false, fmt.Errorf("Missing/invalid companion (%s): %s", err.Error(), file.GetRelPath())
	}

	// Sort it.
	if cmp.PrevFile != "" {
		stagedPrevFile := filepath.Join(f.Conf.StageDir, cmp.SourceName, cmp.PrevFile)
		_, err = os.Stat(stagedPrevFile + PartExt)
		if !os.IsNotExist(err) {
			logging.Debug("FINAL Previous File in Progress:", file.GetRelPath(), cmp.PrevFile)
			return false, nil
		}
		_, err = os.Stat(stagedPrevFile)
		if !os.IsNotExist(err) {
			logging.Debug("FINAL Waiting for Previous File:", file.GetRelPath(), cmp.PrevFile)
			return false, nil
		}
		success, found := f.fromCache(cmp.SourceName, cmp.PrevFile)
		if !found {
			found = logging.FindReceived(cmp.PrevFile, time.Now(), time.Now().Add(-LogSearch), cmp.SourceName)
			if !found {
				logging.Debug("FINAL Previous File Not Found in Log:", file.GetRelPath(), cmp.PrevFile)
				return false, nil
			}
		} else if !success {
			logging.Debug("FINAL Previous File Failed:", file.GetRelPath(), cmp.PrevFile)
			return false, nil
		}
	}

	// Validate it.
	hash, err := fileutils.FileMD5(file.GetPath(true))
	if err != nil {
		return false, fmt.Errorf("Failed to calculate MD5 of %s: %s", file.GetRelPath(), err.Error())
	}
	if hash != cmp.Hash {
		f.toCache(cmp.SourceName, file, false)
		cmp.Delete()
		os.Remove(file.GetPath(false))
		return false, fmt.Errorf("Failed validation: %s", file.GetPath(false))
	}

	logging.Debug("FINAL Finalizing", cmp.SourceName, file.GetRelPath())

	// Move it.
	finalPath := filepath.Join(f.Conf.FinalDir, cmp.SourceName, file.GetRelPath())
	os.MkdirAll(filepath.Dir(finalPath), os.ModePerm)
	err = os.Rename(file.GetPath(false), finalPath)
	if err != nil {
		return false, fmt.Errorf("Failed to move %s to %s: %s", file.GetPath(false), finalPath, err.Error())
	}

	// Log it.
	logging.Received(file.GetRelPath(), hash, info.Size(), cmp.SourceName)

	// Remember it.
	f.toCache(cmp.SourceName, file, true)

	// Clean it.
	err = cmp.Delete()
	if err != nil {
		return true, fmt.Errorf("Failed to remove companion file for %s: %s", cmp.Path, err.Error())
	}

	return true, nil
}

func (f *Finalizer) fromCache(source string, relPath string) (bool, bool) {
	f.cacheLock.RLock()
	defer f.cacheLock.RUnlock()
	file, exists := f.cache[fmt.Sprintf("%s/%s", source, relPath)]
	if exists {
		return file.success, true
	}
	return false, false
}

func (f *Finalizer) toCache(source string, file ScanFile, success bool) {
	f.cacheLock.Lock()
	defer f.cacheLock.Unlock()
	now := time.Now()
	ff := &finalFile{file: file, time: now, success: success}
	f.cache[fmt.Sprintf("%s/%s", source, file.GetRelPath())] = ff
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
