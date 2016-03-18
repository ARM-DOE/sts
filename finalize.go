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

// LogSearchDays is how far back (max) to look in the logs for a finalized file.
const LogSearchDays = 30

// Finalizer manages files that need to be validated against companions.
type Finalizer struct {
	Conf      *ReceiverConf
	last      int64
	cache     map[string]*finalFile
	cacheLock sync.RWMutex
}

type finalFile struct {
	file    ScanFile
	time    int64
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
		files := <-inChan
		for _, file := range files {
			_, err := f.finalize(file)
			if err != nil {
				logging.Error(err.Error())
			}
		}
	}
}

// IsFinal is for outside components to ask if the specified file has been finalized.
func (f *Finalizer) IsFinal(source string, relPath string, sentTime int64) (bool, bool) {
	success, found := f.fromCache(source, relPath)
	if !found {
		success = logging.GetLogger(logging.Receive).Search(relPath, 0, sentTime, source)
		return success, success
	}
	return success, true
}

func (f *Finalizer) finalize(file ScanFile) (bool, error) {
	// Check it.
	info, err := os.Stat(file.GetPath())
	if os.IsNotExist(err) {
		return false, nil // Was probably just picked up by the scanner again before getting moved.
	} else if err != nil {
		return false, fmt.Errorf(err.Error())
	}

	// Read about it.
	cmp, err := ReadCompanion(file.GetPath())
	if err != nil {
		os.Remove(file.GetPath())
		return false, fmt.Errorf("Missing companion: %s", file.GetRelPath())
	}

	// Sort it.
	if cmp.PrevFile != "" {
		stagedPrevFile := filepath.Join(f.Conf.StageDir, cmp.SourceName, cmp.PrevFile)
		_, err = os.Stat(stagedPrevFile + PartExt)
		if !os.IsNotExist(err) {
			return false, nil
		}
		_, err = os.Stat(stagedPrevFile)
		if !os.IsNotExist(err) {
			return false, nil
		}
		success, found := f.fromCache(cmp.SourceName, cmp.PrevFile)
		if !found {
			found = logging.GetLogger(logging.Receive).Search(cmp.PrevFile, LogSearchDays, 0, cmp.SourceName)
			if !found {
				logging.Debug("FINAL Not Found in Logs", file.GetRelPath(), cmp.PrevFile)
				return false, nil
			}
		} else if !success {
			return false, nil
		}
	}

	// Validate it.
	hash, err := fileutils.FileMD5(file.GetPath())
	if err != nil {
		return false, fmt.Errorf("Failed to calculate MD5 of %s: %s", file.GetRelPath(), err.Error())
	}
	if hash != cmp.Hash {
		f.toCache(cmp.SourceName, file, false)
		cmp.Delete()
		return false, fmt.Errorf("Failed validation: %s", file.GetRelPath())
	}

	logging.Debug("FINAL Finalizing", file.GetRelPath())

	// Move it.
	finalPath := filepath.Join(f.Conf.FinalDir, cmp.SourceName, file.GetRelPath())
	os.MkdirAll(filepath.Dir(finalPath), os.ModePerm)
	err = os.Rename(file.GetPath(), finalPath)
	if err != nil {
		return false, fmt.Errorf("Failed to move %s to %s: %s", file.GetRelPath(), finalPath, err.Error())
	}

	// Log it.
	logging.Received(file.GetRelPath(), hash, info.Size(), cmp.SourceName)

	// Remember it.
	f.toCache(cmp.SourceName, file, true)

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
	ff := &finalFile{file, time.Now().Unix(), success}
	f.cache[fmt.Sprintf("%s/%s", source, file.GetRelPath())] = ff
	if success {
		f.last = ff.time
	}
	if len(f.cache)%50 == 0 {
		limit := time.Now().Unix() - int64(CacheAge.Seconds())
		for key, cfile := range f.cache {
			if cfile.time < limit {
				logging.Debug("FINAL Clean Cache:", key)
				delete(f.cache, key)
			}
		}
	}
}
