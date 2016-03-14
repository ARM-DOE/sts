package main

import (
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ARM-DOE/sts/logging"
)

type sortedFile struct {
	file      ScanFile
	tag       string
	hash      string
	nextByTag SortFile
	prevByTag SortFile
	next      SortFile
	prev      SortFile
}

func newSortFile(foundFile ScanFile) *sortedFile {
	file := &sortedFile{}
	file.file = foundFile
	file.tag = strings.Split(foundFile.GetRelPath(), ".")[0]
	return file
}

func (file *sortedFile) GetPath() string {
	return file.file.GetPath()
}

func (file *sortedFile) GetRelPath() string {
	return file.file.GetRelPath()
}

func (file *sortedFile) GetSize() int64 {
	return file.file.GetSize()
}

func (file *sortedFile) GetTime() int64 {
	return file.file.GetTime()
}

func (file *sortedFile) GetTag() string {
	return file.tag
}

func (file *sortedFile) GetHash() string {
	return file.hash
}

func (file *sortedFile) GetNext() SortFile {
	return file.next
}

func (file *sortedFile) GetPrev() SortFile {
	return file.prev
}

func (file *sortedFile) SetNext(next SortFile) {
	file.next = next
}

func (file *sortedFile) SetPrev(prev SortFile) {
	file.prev = prev
}

func (file *sortedFile) GetNextByTag() SortFile {
	return file.nextByTag
}

func (file *sortedFile) GetPrevByTag() SortFile {
	return file.prevByTag
}

func (file *sortedFile) SetNextByTag(next SortFile) {
	file.nextByTag = next
}

func (file *sortedFile) SetPrevByTag(prev SortFile) {
	file.prevByTag = prev
}

func (file *sortedFile) InsertAfter(prev SortFile) {
	file.SetNext(prev.GetNext())
	file.SetPrev(prev)
	prev.SetNext(file)
}

func (file *sortedFile) InsertAfterByTag(prev SortFile) {
	file.SetNextByTag(prev.GetNextByTag())
	file.SetPrevByTag(prev)
	prev.SetNextByTag(file)
}

func (file *sortedFile) InsertBefore(prev SortFile) {
	file.SetNext(prev)
	file.SetPrev(prev.GetPrev())
	prev.SetPrev(file)
}

func (file *sortedFile) InsertBeforeByTag(prev SortFile) {
	file.SetNextByTag(prev)
	file.SetPrevByTag(prev.GetPrevByTag())
	prev.SetPrevByTag(file)
}

// Sorter is responsible for sorting files based on tag configuration.
type Sorter struct {
	rootPath  string
	files     map[string]SortFile
	tags      map[string]*Tag
	tagData   []*Tag
	tagDef    *Tag
	next      SortFile
	last      SortFile
	lastByTag map[string]SortFile
}

// NewSorter returns a new Sorter instance with input tag configuration.
func NewSorter(tagData []*Tag) *Sorter {
	sorter := &Sorter{}
	sorter.files = make(map[string]SortFile)
	sorter.tags = make(map[string]*Tag)
	sorter.tagData = tagData
	for _, tag := range tagData {
		if tag.Pattern == DefaultTag {
			sorter.tagDef = tag
			break
		}
	}
	sorter.lastByTag = make(map[string]SortFile)
	return sorter
}

// Start starts the daemon that listens for files on inChan and doneChan and puts files on outChan.
func (sorter *Sorter) Start(inChan chan []ScanFile, outChan chan SortFile, doneChan chan DoneFile) {
	for {
		select {
		case foundFiles := <-inChan:
			for _, foundFile := range foundFiles {
				logging.Debug("SORT In:", foundFile.GetRelPath())
				sorter.add(foundFile)
			}
		case file := <-doneChan:
			logging.Debug("SORT Done:", file.GetSortFile().GetRelPath())
			sorter.done(file)
		default:
			if sorter.next != nil {
				logging.Debug("SORT Out:", sorter.next.GetRelPath())
				select { // We don't want to block if the send queue is full so we can keep accepting incoming files.
				case outChan <- sorter.next:
					if sorter.next.GetNext() != nil {
						sorter.next = sorter.next.GetNext()
					} else {
						sorter.next = nil
						sorter.last = nil
					}
					continue
				default:
					break
				}
			}
			time.Sleep(100 * time.Millisecond) // To avoid thrashing.
		}
	}
}

func (sorter *Sorter) add(f ScanFile) {
	_, inMap := sorter.files[f.GetPath()]
	if inMap {
		return
	}
	file := newSortFile(f)
	_, exists := sorter.lastByTag[file.GetTag()]
	if !exists {
		sorter.lastByTag[file.GetTag()] = file
		found := false
		for _, tag := range sorter.tagData {
			if tag.Pattern == DefaultTag {
				continue
			}
			matched, err := regexp.MatchString(tag.Pattern, file.GetTag())
			if matched && err == nil {
				sorter.tags[file.GetTag()] = tag
				found = true
				break
			}
		}
		if !found {
			sorter.tags[file.GetTag()] = sorter.tagDef
		}
	}
	if sorter.last == nil {
		sorter.next = file
		sorter.last = file
	} else {
		sorter.insertFile(sorter.last, file)
		sorter.insertFileByTag(sorter.lastByTag[file.GetTag()], file)
		if file.GetPrev() == sorter.last {
			sorter.last = file
		}
		if file.GetPrevByTag() == sorter.lastByTag[file.GetTag()] {
			sorter.lastByTag[file.GetTag()] = file
		}
	}
	sorter.files[file.GetPath()] = file
}

func (sorter *Sorter) insertFile(prev SortFile, file SortFile) {
	tag := sorter.tags[file.GetTag()]
	for {
		tagPrev := sorter.tags[prev.GetTag()]
		if tag.Priority >= tagPrev.Priority {
			if tag.Order == OrderNone {
				file.InsertAfter(prev)
				break
			} else if tag.Order == OrderFIFO {
				if file.GetTime() >= prev.GetTime() {
					file.InsertAfter(prev)
					break
				}
			}
		}
		if prev == sorter.next {
			file.InsertBefore(prev)
			sorter.next = file
			break
		}
		prev = prev.GetPrev()
	}
}

func (sorter *Sorter) insertFileByTag(prev SortFile, file SortFile) {
	tag := sorter.tags[file.GetTag()]
	if tag.Order != OrderFIFO {
		return
	}
	for {
		if file.GetTime() >= prev.GetTime() {
			file.InsertAfterByTag(prev)
			break
		}
		if sorter.lastByTag[file.GetTag()] == prev {
			file.InsertBeforeByTag(prev)
			sorter.lastByTag[file.GetTag()] = file
			break
		}
		prev = prev.GetPrevByTag()
	}
}

func (sorter *Sorter) done(file DoneFile) {
	f := file.GetSortFile()
	if f.GetPrev() != nil {
		f.GetPrev().SetNext(nil)
	}
	if f.GetPrevByTag() != nil {
		f.GetPrevByTag().SetNextByTag(nil)
	}
	if sorter.tags[f.GetTag()].Delete {
		err := os.Remove(f.GetPath())
		if err != nil {
			logging.Error("Failed to remove:", f.GetPath(), err.Error())
		}
	}
}
