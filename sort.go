package sts

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
}

func newSortedGroup(group string, conf *Tag) *sortedGroup {
	t := &sortedGroup{}
	t.group = group
	t.conf = conf
	return t
}

func (t *sortedGroup) remove() {
	if t.prev != nil {
		t.prev.next = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	}
}

func (t *sortedGroup) addAfter(prev *sortedGroup) {
	t.next = prev.next
	t.prev = prev
	prev.next = t
	if t.next != nil {
		t.next.prev = t
	}
}

func (t *sortedGroup) addBefore(next *sortedGroup) {
	t.next = next
	t.prev = next.prev
	next.prev = t
	if t.prev != nil {
		t.prev.next = t
	}
}

func (t *sortedGroup) insertAfter(prev *sortedGroup) {
	t.remove()
	t.addAfter(prev)
}

func (t *sortedGroup) insertBefore(next *sortedGroup) {
	t.remove()
	t.addBefore(next)
}

type sortedFile struct {
	file  ScanFile
	group *sortedGroup
	next  SortFile
	prev  SortFile
	dirty bool
}

func newSortedFile(foundFile ScanFile) *sortedFile {
	file := &sortedFile{}
	file.file = foundFile
	return file
}

func (file *sortedFile) GetOrigFile() ScanFile {
	return file.file
}

func (file *sortedFile) GetPath(follow bool) string {
	return file.file.GetPath(follow)
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

func (file *sortedFile) Reset() (bool, error) {
	return file.file.Reset()
}

func (file *sortedFile) Invalidate() {
	file.dirty = true
}

func (file *sortedFile) Validate() (changed bool, err error) {
	if file.dirty {
		changed, err = file.Reset()
		file.dirty = false
	}
	return
}

func (file *sortedFile) GetGroup() string {
	return file.group.group
}

func (file *sortedFile) GetNext() SortFile {
	return file.next
}

func (file *sortedFile) GetPrev() SortFile {
	return file.prev
}

func (file *sortedFile) GetPrevReq() SortFile {
	if file.group.conf.Order != OrderNone {
		return file.GetPrev()
	}
	return nil
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
	if file.GetNext() != nil {
		file.GetNext().SetPrev(file)
	}
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
	if file.GetPrev() != nil {
		file.GetPrev().SetNext(file)
	}
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
		if tag.Pattern == nil {
			sorter.tagDef = tag
			break
		}
	}
	return sorter
}

// Start starts the daemon that listens for files on inChan and doneChan and puts files on outChan
// according to the output "method".
func (sorter *Sorter) Start(inChan <-chan []ScanFile, outChan map[string]chan SortFile, doneChan <-chan []DoneFile) {
	for {
		select {
		case foundFiles, ok := <-inChan:
			if !ok {
				inChan = nil
				break
			}
			for _, foundFile := range foundFiles {
				logging.Debug("SORT File In:", foundFile.GetRelPath())
				sorter.add(foundFile)
			}
		case doneFiles, ok := <-doneChan:
			if !ok {
				return // doneChan closed; we can exit now.
			}
			for _, doneFile := range doneFiles {
				logging.Debug("SORT File Done:", doneFile.GetRelPath())
				sorter.done(doneFile)
			}
		default:
			next := sorter.getNext()
			if next != nil {
				grp := sorter.groupMap[next.GetGroup()]
				out, ok := outChan[grp.conf.Method]
				if !ok {
					break // This really shouldn't happen but need to put logic in to catch it anyway.
				}
				// In case it was invalidated at some point, this will get the file stats current.
				// A file can potentially sit in the sort queue for some time and so we want to make
				// sure now that it's ready to be sent that its stats are accurate.
				changed, err := next.Validate()
				if err != nil {
					logging.Error("Failed to validate post-sort:", next.GetRelPath(), err.Error())
					break
				}
				if changed {
					logging.Debug("SORT File Changed:", next.GetRelPath())
				}
				select { // We don't want to block if the send queue is full so we can keep accepting incoming files.
				case out <- next:
					prev := ""
					if next.GetPrev() != nil {
						prev = next.GetPrev().GetRelPath()
					}
					logging.Debug("SORT File Out:", next.GetRelPath(), "<-", prev)
					sorter.clearNext(next)
					continue
				default:
					continue
				}
			} else if inChan == nil && outChan != nil {
				for _, ch := range outChan {
					if ch != nil {
						close(ch) // Only close once there are no more in the queue.
					}
				}
				outChan = nil
			}
			time.Sleep(100 * time.Millisecond) // To avoid thrashing.
			break
		}
	}
}

// Add adds a new file to the list.
func (sorter *Sorter) add(f ScanFile) {
	var exists bool
	_, exists = sorter.files[f.GetPath(false)]
	if exists {
		return
	}
	file := newSortedFile(f)
	group := sorter.getGroup(file.GetRelPath())
	sgrp, exists := sorter.groupMap[group]
	if !exists {
		conf := sorter.getGroupConf(group)
		sgrp = newSortedGroup(group, conf)
		sorter.groupMap[group] = sgrp
		sorter.addGroup(sgrp)
	}
	file.group = sgrp
	sorter.addFile(file)
	sorter.files[file.GetPath(false)] = file
}

func (sorter *Sorter) getGroup(relPath string) string {
	m := sorter.groupBy.FindStringSubmatch(relPath)
	if len(m) > 1 {
		logging.Debug("SORT Group:", m[1])
		return m[1]
	}
	return ""
}

func (sorter *Sorter) getNext() SortFile {
	next := sorter.nextOut
	if next == nil {
		g := sorter.group
		for g != nil {
			if sorter.next[g.group] != nil {
				// Make this file next one out the door.
				next = sorter.next[g.group]
				// But first we need to make sure we shouldn't skip this one...
				if g.conf.LastDelay > 0 && next.GetNext() == nil {
					next.Reset() // This is to make sure we get the current mod time.
					if time.Now().Sub(time.Unix(next.GetTime(), 0)) < g.conf.LastDelay {
						// This is the last file in the group and it's not old
						// enough to send yet.  And we need to "invalidate" it
						// in order to get proper file stats when it finally is
						// ready to be sent.  If we don't do this then if the file
						// had been growing, we could easily have an incorrect
						// size value for the file and it would eventually fail
						// and have to be resent.
						next.Invalidate()
						next = nil
						g = g.next
						continue
					}
				}
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
	// Find last group of same priority.
	for n.next != nil && n.next.conf.Priority == p {
		n = n.next
	}
	if n == g {
		return // We only have one group of this priority.
	}
	if g == sorter.group {
		// Point to the next one if we're moving the first group.
		sorter.group = g.next
	}
	// Move this group after the last one of same priority.
	logging.Debug("SORT Moved Group:", n.group, g.group)
	g.insertAfter(n)
}

func (sorter *Sorter) getGroupConf(g string) *Tag {
	conf := sorter.tagDef
	for _, tag := range sorter.tagData {
		if tag.Pattern == nil { // DEFAULT
			continue
		}
		if tag.Pattern.MatchString(g) {
			conf = tag
			break
		}
	}
	return conf
}

func (sorter *Sorter) addGroup(grp *sortedGroup) {
	g := sorter.group
	if g == nil {
		sorter.group = grp
		return
	}
	for g != nil {
		if grp.conf.Priority > g.conf.Priority {
			grp.addBefore(g)
			if g == sorter.group {
				sorter.group = grp
			}
			break
		}
		if g.next == nil {
			grp.addAfter(g)
			break
		}
		g = g.next
	}
}

func (sorter *Sorter) addFile(file SortFile) {
	grp := sorter.groupMap[file.GetGroup()]
	f := sorter.next[grp.group]
	if f == nil {
		sorter.next[grp.group] = file
		return
	}
	n := 0
loop:
	for f != nil {
		switch grp.conf.Order {
		case OrderFIFO:
			if file.GetTime() < f.GetTime() {
				file.InsertBefore(f)
				break loop
			}
			if file.GetTime() == f.GetTime() {
				// It's important that the sorting be the same every time in order to recover
				// gracefully when order matters for delivery.  In other words, if a file has
				// a required previous file the first time through, it better have the same one
				// (or none) if passed through again after a crash recovery.  This is why we're
				// also doing an alpha sort in addition to mod time because mod times can match
				// but names cannot.
				if file.GetRelPath() < f.GetRelPath() {
					file.InsertBefore(f)
					break loop
				}
			}
			break
		case OrderLIFO:
			if file.GetTime() > f.GetTime() {
				logging.Debug("SORT LIFO:", file.GetTime(), "->", f.GetTime())
				file.InsertBefore(f)
				break loop
			}
			if file.GetTime() == f.GetTime() {
				// Same reasoning as above.
				if file.GetRelPath() < f.GetRelPath() {
					file.InsertBefore(f)
					break loop
				}
			}
			break
		default: // No ordering
			break loop
		}
		n++
		if f.GetNext() == nil {
			logging.Debug("SORT:", grp.conf.Order, f.GetTime(), "->", file.GetTime())
			file.InsertAfter(f)
			break
		}
		f = f.GetNext()
	}
	// If we inserted before the head file we need to update the pointer.
	if n == 0 {
		sorter.next[grp.group] = file
	}
}

// Done will remove this file from the list and do any cleanup as specified by the tag.
func (sorter *Sorter) done(file DoneFile) {
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
	if del && file.GetSuccess() {
		logging.Debug("SORT Delete:", file.GetPath())
		err := os.Remove(file.GetPath())
		if err != nil {
			logging.Error("Failed to remove:", file.GetPath(), err.Error())
		}
	}
}
