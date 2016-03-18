package main

import (
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ARM-DOE/sts/logging"
)

// computeTag returns the "tag" - the relative path to the first dot.
// TODO: look into making this more generic.
func computeTag(relPath string) string {
	return strings.Split(relPath, ".")[0]
}

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
	file.tag = computeTag(file.GetRelPath())
	return file
}

func (file *sortedFile) GetOrigFile() ScanFile {
	return file.file
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
func (sorter *Sorter) Start(inChan chan []ScanFile, outChan chan SortFile, doneChan chan []DoneFile) {
	for {
		select {
		case foundFiles := <-inChan:
			for _, foundFile := range foundFiles {
				logging.Debug("SORT File In:", foundFile.GetRelPath())
				sorter.Add(foundFile)
			}
		case doneFiles := <-doneChan:
			for _, doneFile := range doneFiles {
				logging.Debug("SORT File Done:", doneFile.GetRelPath())
				sorter.Done(doneFile)
			}
		default:
			next := sorter.getNext()
			if next != nil {
				select { // We don't want to block if the send queue is full so we can keep accepting incoming files.
				case outChan <- next:
					logging.Debug("SORT File Out:", next.GetRelPath())
					sorter.clearNext(next)
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

// Add adds a new file to the list.
func (sorter *Sorter) Add(f ScanFile) {
	var exists bool
	_, exists = sorter.files[f.GetPath()]
	if exists {
		return
	}
	file := newSortedFile(f)
	ftag := file.GetTag()
	_, exists = sorter.tagMap[ftag]
	if !exists {
		conf := sorter.getTagConf(ftag)
		stag := newSortedTag(ftag, conf)
		sorter.tagMap[ftag] = stag
		sorter.insertTag(stag)
	}
	sorter.insertFile(file)
	sorter.files[file.GetPath()] = file
}

func (sorter *Sorter) getNext() SortFile {
	next := sorter.nextOut
	if next == nil {
		t := sorter.tag
		for t != nil {
			if sorter.next[t.tag] != nil {
				// Make this file next one out the door.
				next = sorter.next[t.tag]
				// Push this tag to the last one of its priority to avoid starvation.
				sorter.delayTag(t)
				break
			}
			t = t.next
		}
		sorter.nextOut = next
	}
	return next
}

func (sorter *Sorter) clearNext(out SortFile) {
	next := out.GetNext()
	ntag := out.GetTag()
	sorter.next[ntag] = next
	sorter.nextOut = nil
}

func (sorter *Sorter) delayTag(t *sortedTag) {
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
}

func (sorter *Sorter) getTagConf(t string) *Tag {
	conf := sorter.tagDef
	for _, tag := range sorter.tagData {
		if tag.Pattern == DefaultTag {
			continue
		}
		matched, err := regexp.MatchString(tag.Pattern, t)
		if matched && err == nil {
			conf = tag
			break
		}
	}
	return conf
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

// Done will remove this file from the list and do any cleanup as specified by the tag.
func (sorter *Sorter) Done(file DoneFile) {
	del := false
	f, found := sorter.files[file.GetPath()]
	if found {
		if f.GetPrev() != nil {
			f.GetPrev().SetNext(nil)
		}
		if sorter.tagMap[f.GetTag()].conf.Delete {
			del = true
		}
		delete(sorter.files, file.GetPath())
	} else {
		tag := sorter.getTagConf(computeTag(file.GetRelPath()))
		del = tag.Delete
	}
	if del {
		err := os.Remove(file.GetPath())
		if err != nil {
			logging.Error("Failed to remove:", file.GetPath(), err.Error())
		}
	}
}
