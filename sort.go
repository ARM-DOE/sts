package main

import (
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ARM-DOE/sts/logging"
)

type sortedTag struct {
	tag   string
	conf  *Tag
	next  *sortedTag
	prev  *sortedTag
	bytes int64
	files int
	time  int64
}

func newSortedTag(tag string, conf *Tag) *sortedTag {
	t := &sortedTag{}
	t.tag = tag
	t.conf = conf
	return t
}

func (t *sortedTag) insertAfter(prev *sortedTag) {
	if t.prev != nil {
		t.prev.next = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	}
	t.next = prev.next
	t.prev = prev
	prev.next = t
}

func (t *sortedTag) insertBefore(next *sortedTag) {
	if t.prev != nil {
		t.prev.next = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	}
	t.next = next
	t.prev = next.prev
	next.prev = t
}

type sortedFile struct {
	file ScanFile
	tag  string
	hash string
	next SortFile
	prev SortFile
}

func newSortedFile(foundFile ScanFile) *sortedFile {
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

func (file *sortedFile) InsertAfter(prev SortFile) {
	if file.GetPrev() != nil {
		file.GetPrev().SetNext(file.GetNext())
	}
	if file.GetNext() != nil {
		file.GetNext().SetPrev(file.GetPrev())
	}
	file.SetNext(prev.GetNext())
	file.SetPrev(prev)
	prev.SetNext(file)
}

func (file *sortedFile) InsertBefore(next SortFile) {
	if file.GetPrev() != nil {
		file.GetPrev().SetNext(file.GetNext())
	}
	if file.GetNext() != nil {
		file.GetNext().SetPrev(file.GetPrev())
	}
	file.SetNext(next)
	file.SetPrev(next.GetPrev())
	next.SetPrev(file)
}

// Sorter is responsible for sorting files based on tag configuration.
type Sorter struct {
	rootPath string
	files    map[string]SortFile   // path -> file pointer (lookup)
	next     map[string]SortFile   // tag -> file pointer (next by tag)
	nextOut  SortFile              // the next file out the door
	tag      *sortedTag            // the first tag by priority
	tagMap   map[string]*sortedTag // tag -> tag conf
	tagData  []*Tag                // list of raw tag conf
	tagDef   *Tag                  // default tag
}

// NewSorter returns a new Sorter instance with input tag configuration.
func NewSorter(tagData []*Tag) *Sorter {
	sorter := &Sorter{}
	sorter.files = make(map[string]SortFile)
	sorter.next = make(map[string]SortFile)
	sorter.tagMap = make(map[string]*sortedTag)
	sorter.tagData = tagData
	for _, tag := range tagData {
		if tag.Pattern == DefaultTag {
			sorter.tagDef = tag
			break
		}
	}
	return sorter
}

// Start starts the daemon that listens for files on inChan and doneChan and puts files on outChan.
func (sorter *Sorter) Start(inChan chan []ScanFile, outChan chan SortFile, doneChan chan DoneFile) {
	for {
		select {
		case foundFiles := <-inChan:
			for _, foundFile := range foundFiles {
				logging.Debug("SORT File In:", foundFile.GetRelPath())
				sorter.add(foundFile)
			}
		case file := <-doneChan:
			logging.Debug("SORT File Done:", file.GetSortFile().GetRelPath())
			sorter.done(file)
		default:
			if sorter.nextOut == nil {
				t := sorter.tag
				for t != nil {
					if sorter.next[t.tag] != nil {
						// Make this file next one out the door.
						sorter.nextOut = sorter.next[t.tag]
						// Push this tag to the last one of its priority to avoid starvation.
						p := t.conf.Priority
						n := t
						for n.next != nil && n.next.conf.Priority == p {
							n = n.next
						}
						if n != t {
							if t == sorter.tag {
								sorter.tag = t.next
							}
							if n.conf.Priority < t.conf.Priority {
								t.insertBefore(n)
								if n == sorter.tag {
									sorter.tag = t
								}
							} else {
								t.insertAfter(n)
							}
						}
						break
					}
					t = t.next
				}
			}
			if sorter.nextOut != nil {
				select { // We don't want to block if the send queue is full so we can keep accepting incoming files.
				case outChan <- sorter.nextOut:
					logging.Debug("SORT File Out:", sorter.nextOut.GetRelPath())
					next := sorter.nextOut.GetNext()
					ntag := sorter.nextOut.GetTag()
					sorter.next[ntag] = next
					sorter.nextOut = nil
					continue
				default:
					continue
				}
			}
			time.Sleep(100 * time.Millisecond) // To avoid thrashing.
			break
		}
	}
}

func (sorter *Sorter) add(f ScanFile) {
	var exists bool
	_, exists = sorter.files[f.GetPath()]
	if exists {
		return
	}
	file := newSortedFile(f)
	ftag := file.GetTag()
	_, exists = sorter.tagMap[ftag]
	if !exists {
		conf := sorter.tagDef
		for _, tag := range sorter.tagData {
			if tag.Pattern == DefaultTag {
				continue
			}
			matched, err := regexp.MatchString(tag.Pattern, ftag)
			if matched && err == nil {
				conf = tag
				break
			}
		}
		stag := newSortedTag(ftag, conf)
		sorter.tagMap[ftag] = stag
		sorter.insertTag(stag)
	}
	sorter.insertFile(file)
	sorter.files[file.GetPath()] = file
}

func (sorter *Sorter) insertTag(tag *sortedTag) {
	t := sorter.tag
	if t == nil {
		sorter.tag = tag
		return
	}
	for t != nil {
		if tag.conf.Priority > t.conf.Priority {
			tag.insertBefore(t)
			if t == sorter.tag {
				sorter.tag = tag
			}
			break
		}
		if t.next == nil {
			tag.insertAfter(t)
			break
		}
		t = t.next
	}
}

func (sorter *Sorter) insertFile(file SortFile) {
	tag := sorter.tagMap[file.GetTag()]
	if tag.conf.Order == OrderNone {
		return
	}
	f := sorter.next[tag.tag]
	if f == nil {
		sorter.next[tag.tag] = file
		return
	}
	for f != nil {
		if tag.conf.Order == OrderFIFO && file.GetTime() < f.GetTime() {
			file.InsertBefore(f)
			if f == sorter.next[tag.tag] {
				sorter.next[tag.tag] = file
			}
			break
		}
		if f.GetNext() == nil {
			file.InsertAfter(f)
			break
		}
		f = f.GetNext()
	}
}

func (sorter *Sorter) done(file DoneFile) {
	f := file.GetSortFile()
	if f.GetPrev() != nil {
		f.GetPrev().SetNext(nil)
	}
	if sorter.tagMap[f.GetTag()].conf.Delete {
		err := os.Remove(f.GetPath())
		if err != nil {
			logging.Error("Failed to remove:", f.GetPath(), err.Error())
		}
	}
}
