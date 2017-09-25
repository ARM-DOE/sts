package sort

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/conf"
	"code.arm.gov/dataflow/sts/logging"
)

type sortedGroup struct {
	group string
	conf  *conf.Tag
	next  *sortedGroup
	prev  *sortedGroup
}

func newSortedGroup(group string, conf *conf.Tag) *sortedGroup {
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

type fileShare struct {
	orig  sts.ScanFile
	alloc int64
	sent  int64
	lock  sync.RWMutex
}

func (s *fileShare) setNextAlloc() bool {
	if rf, ok := s.orig.(sts.RecoverFile); ok {
		beg, end := rf.GetNextAlloc()
		if end-beg > file.chunkLength {
			end = beg + file.chunkLength
		}
		rf.AddAlloc(end - beg)
		return end >= file.GetSize()
	}
	file.chunkOffset += file.chunkLength
	return file.chunkOffset+file.chunkLength >= file.GetSize()
}

func (file *sortedFile) IsLast() bool {
	return !(file.chunkOffset+file.chunkLength < file.GetSize())
}

func (file *sortedFile) IsSent() bool {
	file.lock.RLock()
	defer file.lock.RUnlock()
	if rf, ok := file.file.(sts.RecoverFile); ok {
		return rf.IsSent()
	}
	return file.isSent()
}

func (file *sortedFile) isSent() bool {
	return file.sent == file.GetSize()
}

func (file *sortedFile) AddSent(bytes int64) bool {
	file.lock.Lock()
	defer file.lock.Unlock()
	if rf, ok := file.file.(sts.RecoverFile); ok {
		rf.AddBytesSent(bytes)
		return rf.IsSent()
	}
	file.sent += bytes
	return file.isSent()
}

func (file *sortedFile) SetStarted(t time.Time) {
	file.lock.Lock()
	defer f.lock.Unlock()
	if file.started.IsZero() || t.Before(file.started) {
		file.started = t
	}
}

func (file *sortedFile) GetStarted() time.Time {
	file.lock.RLock()
	defer file.lock.RUnlock()
	return file.started
}

func (file *sortedFile) SetCompleted(t time.Time) {
	file.lock.Lock()
	defer file.lock.Unlock()
	if t.After(f.completed) {
		file.completed = t
	}
}

func (file *sortedFile) GetCompleted() time.Time {
	file.lock.RLock()
	defer file.lock.RUnlock()
	return file.completed
}

func (file *sortedFile) TimeMs() int64 {
	file.lock.RLock()
	defer file.lock.RUnlock()
	if !file.completed.IsZero() && !file.started.IsZero() {
		return int64(f.completed.Sub(f.started).Nanoseconds() / 1e6)
	}
	return -1
}

type sortedFile struct {
	file        sts.ScanFile
	group       *sortedGroup
	next        *sortedFile
	prev        *sortedFile
	dirty       bool
	prevName    string
	chunkOffset int64
	chunkLength int64
	lock        sync.RWMutex
}

func newSortedFile(foundFile sts.ScanFile) *sortedFile {
	file := &sortedFile{}
	file.file = foundFile
	file.chunkLength = foundFile.GetSize()
	return file
}

// func (file *sortedFile) GetOrigFile() sts.ScanFile {
// 	return file.file
// }

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

func (file *sortedFile) GetHash() string {
	return file.file.GetHash()
}

func (file *sortedFile) Reset() (bool, error) {
	return file.file.Reset()
}

func (file *sortedFile) GetPrevName() string {
	return file.prevName
}

func (file *sortedFile) invalidate() {
	file.dirty = true
}

func (file *sortedFile) validate() (changed bool, err error) {
	if file.dirty {
		if changed, err = file.Reset(); err != nil {
			return
		}
		file.dirty = false
	}
	return
}

func (file *sortedFile) getGroup() string {
	return file.group.group
}

func (file *sortedFile) getNext() *sortedFile {
	return file.next
}

func (file *sortedFile) getPrev() *sortedFile {
	return file.prev
}

func (file *sortedFile) setNext(next *sortedFile) {
	file.next = next
}

func (file *sortedFile) setPrev(prev *sortedFile) {
	file.prev = prev
}

func (file *sortedFile) insertAfter(prev *sortedFile) {
	if file.getPrev() != nil {
		file.getPrev().setNext(file.getNext())
	}
	if file.getNext() != nil {
		file.getNext().setPrev(file.getPrev())
	}
	file.setNext(prev.getNext())
	file.setPrev(prev)
	prev.setNext(file)
	if file.getNext() != nil {
		file.getNext().setPrev(file)
	}
}

func (file *sortedFile) insertBefore(next *sortedFile) {
	if file.getPrev() != nil {
		file.getPrev().setNext(file.getNext())
	}
	if file.getNext() != nil {
		file.getNext().setPrev(file.getPrev())
	}
	file.setNext(next)
	file.setPrev(next.getPrev())
	next.setPrev(file)
	if file.getPrev() != nil {
		file.getPrev().setNext(file)
	}
}

func (file *sortedFile) unlink() {
	prev := file.getPrev()
	next := file.getNext()
	if prev != nil {
		prev.setNext(next)
		file.setPrev(nil)
	}
	if next != nil {
		next.setPrev(prev)
		file.setNext(nil)
	}
}

// Sorter is responsible for sorting files based on tag configuration.
type Sorter struct {
	rootPath  string
	groupBy   *regexp.Regexp          // the pattern that puts files into groups to avoid starvation
	files     map[string]*sortedFile  // path -> file pointer (lookup)
	head      map[string]*sortedFile  // group -> file pointer (first by group)
	group     *sortedGroup            // the first group by priority
	groupMap  map[string]*sortedGroup // group -> group conf (tag)
	tagData   []*conf.Tag             // list of raw tag conf
	tagDef    *conf.Tag               // default tag
	chunkSize int64                   // max size to send out to better honor priorities
}

// NewSorter returns a new Sorter instance with input tag configuration.
func NewSorter(tagData []*conf.Tag, groupBy *regexp.Regexp) *Sorter {
	sorter := &Sorter{}
	sorter.groupBy = groupBy
	sorter.files = make(map[string]*sortedFile)
	sorter.head = make(map[string]*sortedFile)
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

// SetChunkSize sets the maximum byte count to be sent out at a time for a single file.
func (sorter *Sorter) SetChunkSize(chunkSize int64) {
	sorter.chunkSize = chunkSize
}

// Start starts the daemon that listens for files on inChan and doneChan and puts files on outChan
// according to the output "method".
func (sorter *Sorter) Start(inChan <-chan []sts.ScanFile, outChan map[string]chan sts.SortFile) {
	var next *sortedFile
	for {
		select {
		case foundFiles, ok := <-inChan:
			if !ok {
				inChan = nil
				break
			}
			for _, foundFile := range foundFiles {
				if rf, ok := foundFile.(sts.RecoverFile); ok && rf.IsSent() {
					// TODO... Need to log and then clean up...
				}
				// logging.Debug("SORT File In:", foundFile.GetRelPath())
				sorter.add(foundFile)
			}
			next = nil
		default:
			if next == nil {
				next = sorter.getNext()
			}
			if next != nil {
				grp := sorter.groupMap[next.getGroup()]
				out, ok := outChan[grp.conf.Method]
				if !ok {
					break // This really shouldn't happen but need to put logic in to catch it anyway.
				}
				// In case it was invalidated at some point, this will get the file stats current.
				// A file can potentially sit in the sort queue for some time and so we want to make
				// sure now that it's ready to be sent that its stats are accurate.
				changed, err := next.validate()
				if err != nil {
					logging.Error("Failed to validate post-sort:", next.GetRelPath(), err.Error())
					break
				}
				if changed {
					logging.Debug("SORT File Changed:", next.GetRelPath())
				}
				// Check if there is room on the queue before forcing this one out the door.
				if len(out) < cap(out) {
					sorter.sendNext(grp, out, next)
					next = nil
					continue
				}
			} else if inChan == nil && outChan != nil {
				for _, ch := range outChan {
					if ch != nil {
						close(ch) // Only close once there are no more in the queue.
					}
				}
				return
			}
			time.Sleep(10 * time.Millisecond) // To avoid thrashing.
			break
		}
	}
}

// Add adds a new file to the list.
func (sorter *Sorter) add(f sts.ScanFile) {
	var exists bool
	_, exists = sorter.files[f.GetPath(false)]
	if exists {
		return
	}
	group := sorter.getGroup(f.GetRelPath())
	sgrp, exists := sorter.groupMap[group]
	if !exists {
		conf := sorter.getGroupConf(group)
		if conf == nil {
			logging.Debug("SORT Ignore:", f.GetRelPath(), "(no matching tag configuration)")
			return
		}
		sgrp = newSortedGroup(group, conf)
		sorter.groupMap[group] = sgrp
		sorter.addGroup(sgrp)
	}
	file := newSortedFile(f)
	file.group = sgrp
	sorter.addFile(file)
	sorter.files[file.GetPath(false)] = file
}

func (sorter *Sorter) getGroup(relPath string) string {
	m := sorter.groupBy.FindStringSubmatch(relPath)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

func (sorter *Sorter) getNext() *sortedFile {
	g := sorter.group
	var next *sortedFile
	for g != nil {
		next = sorter.head[g.group]
		if next == nil {
			g = g.next
			continue
		}
		// And then we need to make sure we shouldn't skip this one...
		if g.conf.LastDelay > 0 && next.getNext() == nil {
			next.Reset() // This is to make sure we get the current mod time.
			if time.Now().Sub(time.Unix(next.GetTime(), 0)) < g.conf.LastDelay {
				// This is the last file in the group and it's not old
				// enough to send yet.  And we need to "invalidate" it
				// in order to get proper file stats when it finally is
				// ready to be sent.  If we don't do this then if the file
				// had been growing, we could easily have an incorrect
				// size value for the file and it would eventually fail
				// and have to be resent.
				next.invalidate()
				next = nil
				g = g.next
				continue
			}
		}
		// Push this tag to the last one of its priority to avoid starvation.
		sorter.delayGroup(g)
		break
	}
	return next
}

func (sorter *Sorter) sendNext(grp *sortedGroup, out chan sts.SortFile, next *sortedFile) {
	prevName := ""
	if grp.conf.Order != "" && next.getPrev() != nil {
		prevName = next.getPrev().GetRelPath()
	}
	fileDone := next.IsLast()
	if fileDone {
		// Remove from lists
		if _, ok := sorter.files[next.GetPath(false)]; ok {
			delete(sorter.files, next.GetPath(false))
		}
	}
	orig := next
	if !fileDone {
		// Make a new wrapper to keep the slice from changing for the sender.
		next = newSortedFile(next.file)
		next.group = orig.group
		next.offset = orig.offset
		next.length = orig.length
		next.prevName = prevName
		orig.offset += orig.length
	}
	next.prevName = prevName
	logging.Debug(
		fmt.Sprintf(
			"SORT File Out: %s(%d:%d:%d) <- %s",
			next.GetRelPath(),
			next.offset,
			next.length,
			next.GetSize(),
			next.prevName))
	out <- next
	if fileDone {
		// Update the chain
		if sorter.head[grp.group] == orig {
			sorter.head[grp.group] = orig.getNext()
		}
		if orig.getPrev() != nil {
			orig.getPrev().unlink()
		}
		if orig.getNext() == nil {
			orig.unlink()
		}
	}
}

func (sorter *Sorter) delayGroup(g *sortedGroup) {
	p := g.conf.Priority
	n := g
	// Find last group of same priority.
	for n.next != nil && n.next.conf.Priority == p {
		n = n.next
	}
	if n == g {
		// Either we only have one group of this priority or this group is
		// already the last one.  Either way, we don't need to do anything.
		return
	}
	if g == sorter.group {
		// Point to the next one if we're moving the first group.
		sorter.group = g.next
	}
	// Move this group after the last one of same priority.
	logging.Debug("SORT Moved Group:", n.group, g.group)
	g.insertAfter(n)
}

func (sorter *Sorter) getGroupConf(g string) *conf.Tag {
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

func (sorter *Sorter) addFile(file *sortedFile) {
	grp := sorter.groupMap[file.getGroup()]
	f := sorter.head[grp.group]
	if f == nil {
		sorter.head[grp.group] = file
		return
	}
	n := 0
loop:
	for f != nil {
		switch grp.conf.Order {
		case conf.OrderFIFO:
			if file.GetTime() < f.GetTime() {
				logging.Debug("SORT FIFO:", file.GetRelPath(), "->", f.GetRelPath())
				file.insertBefore(f)
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
					logging.Debug("SORT FIFO (alpha):", file.GetRelPath(), "->", f.GetRelPath())
					file.insertBefore(f)
					break loop
				}
			}
			break
		case conf.OrderLIFO:
			if file.GetTime() > f.GetTime() {
				logging.Debug("SORT LIFO:", file.GetRelPath(), "->", f.GetRelPath())
				file.insertBefore(f)
				break loop
			}
			if file.GetTime() == f.GetTime() {
				// Same reasoning as above.
				if file.GetRelPath() > f.GetRelPath() {
					logging.Debug("SORT LIFO (alpha):", file.GetRelPath(), "->", f.GetRelPath())
					file.insertBefore(f)
					break loop
				}
			}
			break
		}
		n++
		if f.getNext() == nil {
			logging.Debug("SORT:", grp.conf.Order, f.GetRelPath(), "->", file.GetRelPath())
			file.insertAfter(f)
			break
		}
		f = f.getNext()
	}
	// If we inserted before the head file we need to update the pointer.
	if n == 0 {
		sorter.head[grp.group] = file
	}
}
