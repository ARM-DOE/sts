package main

import (
	"os"
	"regexp"
	"time"

	"github.com/ARM-DOE/sts/logging"
)

type sortedGroup struct {
	group string
	conf  *Tag
	next  *sortedGroup
	prev  *sortedGroup
	bytes int64
	files int
	time  int64
}

func newSortedGroup(group string, conf *Tag) *sortedGroup {
	t := &sortedGroup{}
	t.group = group
	t.conf = conf
	return t
}

func (t *sortedGroup) insertAfter(prev *sortedGroup) {
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

func (t *sortedGroup) insertBefore(next *sortedGroup) {
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
	file  ScanFile
	group string
	hash  string
	next  SortFile
	prev  SortFile
}

func newSortedFile(foundFile ScanFile) *sortedFile {
	file := &sortedFile{}
	file.file = foundFile
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

func (file *sortedFile) GetGroup() string {
	return file.group
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
	groupBy  *regexp.Regexp          // the pattern that puts files into groups to avoid starvation
	files    map[string]SortFile     // path -> file pointer (lookup)
	next     map[string]SortFile     // group -> file pointer (next by group)
	nextOut  SortFile                // the next file out the door
	group    *sortedGroup            // the first group by priority
	groupMap map[string]*sortedGroup // group -> group conf (tag)
	tagData  []*Tag                  // list of raw tag conf
	tagDef   *Tag                    // default tag
}

// NewSorter returns a new Sorter instance with input tag configuration.
func NewSorter(tagData []*Tag, groupBy *regexp.Regexp) *Sorter {
	sorter := &Sorter{}
	sorter.groupBy = groupBy
	sorter.files = make(map[string]SortFile)
	sorter.next = make(map[string]SortFile)
	sorter.groupMap = make(map[string]*sortedGroup)
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
	file.group = sorter.getGroup(file.GetRelPath())
	_, exists = sorter.groupMap[file.group]
	if !exists {
		conf := sorter.getGroupConf(file.group)
		sgrp := newSortedGroup(file.group, conf)
		sorter.groupMap[file.group] = sgrp
		sorter.insertGroup(sgrp)
	}
	sorter.insertFile(file)
	sorter.files[file.GetPath()] = file
}

func (sorter *Sorter) getGroup(relPath string) string {
	return sorter.groupBy.FindString(relPath)
}

func (sorter *Sorter) getNext() SortFile {
	next := sorter.nextOut
	if next == nil {
		g := sorter.group
		for g != nil {
			if sorter.next[g.group] != nil {
				// Make this file next one out the door.
				next = sorter.next[g.group]
				// Push this tag to the last one of its priority to avoid starvation.
				sorter.delayGroup(g)
				break
			}
			g = g.next
		}
		sorter.nextOut = next
	}
	return next
}

func (sorter *Sorter) clearNext(out SortFile) {
	next := out.GetNext()
	ngrp := out.GetGroup()
	sorter.next[ngrp] = next
	sorter.nextOut = nil
}

func (sorter *Sorter) delayGroup(g *sortedGroup) {
	p := g.conf.Priority
	n := g
	for n.next != nil && n.next.conf.Priority == p {
		n = n.next
	}
	if n != g {
		if g == sorter.group {
			sorter.group = g.next
		}
		if n.conf.Priority < g.conf.Priority {
			g.insertBefore(n)
			if n == sorter.group {
				sorter.group = g
			}
		} else {
			g.insertAfter(n)
		}
	}
}

func (sorter *Sorter) getGroupConf(g string) *Tag {
	conf := sorter.tagDef
	for _, tag := range sorter.tagData {
		if tag.Pattern == DefaultTag {
			continue
		}
		matched, err := regexp.MatchString(tag.Pattern, g)
		if matched && err == nil {
			conf = tag
			break
		}
	}
	return conf
}

func (sorter *Sorter) insertGroup(grp *sortedGroup) {
	g := sorter.group
	if g == nil {
		sorter.group = grp
		return
	}
	for g != nil {
		if grp.conf.Priority > g.conf.Priority {
			grp.insertBefore(g)
			if g == sorter.group {
				sorter.group = grp
			}
			break
		}
		if g.next == nil {
			grp.insertAfter(g)
			break
		}
		g = g.next
	}
}

func (sorter *Sorter) insertFile(file SortFile) {
	grp := sorter.groupMap[file.GetGroup()]
	if grp.conf.Order == OrderNone {
		return
	}
	f := sorter.next[grp.group]
	if f == nil {
		sorter.next[grp.group] = file
		return
	}
	for f != nil {
		if grp.conf.Order == OrderFIFO && file.GetTime() < f.GetTime() {
			file.InsertBefore(f)
			if f == sorter.next[grp.group] {
				sorter.next[grp.group] = file
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
		if sorter.groupMap[f.GetGroup()].conf.Delete {
			del = true
		}
		delete(sorter.files, file.GetPath())
	} else {
		grp := sorter.getGroupConf(sorter.getGroup(file.GetRelPath()))
		del = grp.Delete
	}
	if del {
		logging.Debug("SORT Delete:", file.GetPath())
		err := os.Remove(file.GetPath())
		if err != nil {
			logging.Error("Failed to remove:", file.GetPath(), err.Error())
		}
	}
}
