package queue

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
)

type link interface {
	getPrev() link
	getNext() link
	setPrev(link)
	setNext(link)
}

func unlink(node link) {
	prev := node.getPrev()
	next := node.getNext()
	if !reflect.ValueOf(prev).IsNil() {
		prev.setNext(next)
		node.setPrev(nil)
	}
	if !reflect.ValueOf(next).IsNil() {
		next.setPrev(prev)
		node.setNext(nil)
	}
}

func addAfter(node link, prev link) {
	next := prev.getNext()
	node.setNext(next)
	node.setPrev(prev)
	prev.setNext(node)
	if !reflect.ValueOf(next).IsNil() {
		next.setPrev(node)
	}
}

func addBefore(node link, next link) {
	prev := next.getPrev()
	node.setNext(next)
	node.setPrev(prev)
	next.setPrev(node)
	if !reflect.ValueOf(prev).IsNil() {
		prev.setNext(node)
	}
}

func insertAfter(node link, prev link) {
	unlink(node)
	addAfter(node, prev)
}

func insertBefore(node link, next link) {
	unlink(node)
	addBefore(node, next)
}

type sortedFile struct {
	orig      sts.Hashed
	group     *sortedGroup
	next      *sortedFile
	prev      *sortedFile
	allocated int64
}

func (f *sortedFile) getNext() link {
	return f.next
}

func (f *sortedFile) getPrev() link {
	return f.prev
}

func (f *sortedFile) getPrevName() string {
	if r, ok := f.orig.(sts.Recovered); ok {
		// Keep original intact
		return r.GetPrev()
	}
	if f.prev != nil {
		return f.prev.orig.GetName()
	}
	return ""
}

func (f *sortedFile) setNext(next link) {
	n, ok := next.(*sortedFile)
	if n == nil || !ok {
		f.next = nil
		return
	}
	f.next = n
}

func (f *sortedFile) setPrev(prev link) {
	p, ok := prev.(*sortedFile)
	if p == nil || !ok {
		f.prev = nil
		return
	}
	f.prev = p
}

func (f *sortedFile) unlink() {
	unlink(f)
}

func (f *sortedFile) addAfter(prev *sortedFile) {
	addAfter(f, prev)
}

func (f *sortedFile) addBefore(next *sortedFile) {
	addBefore(f, next)
}

func (f *sortedFile) insertAfter(prev *sortedFile) {
	insertAfter(f, prev)
}

func (f *sortedFile) insertBefore(next *sortedFile) {
	insertBefore(f, next)
}

func (f *sortedFile) allocate(desired int64) (offset int64, length int64) {
	if r, ok := f.orig.(sts.Recovered); ok {
		return r.Allocate(desired)
	}
	offset = f.allocated
	length = desired
	if desired == 0 || offset+length > f.orig.GetSize() {
		length = f.orig.GetSize() - offset
	}
	f.allocated += length
	return
}

func (f *sortedFile) isAllocated() bool {
	if r, ok := f.orig.(sts.Recovered); ok {
		return r.IsAllocated()
	}
	return f.allocated == f.orig.GetSize()
}

type sortedGroup struct {
	name string
	conf *Tag
	next *sortedGroup
	prev *sortedGroup
}

func (g *sortedGroup) getNext() link {
	return g.next
}

func (g *sortedGroup) getPrev() link {
	return g.prev
}

func (g *sortedGroup) setNext(next link) {
	n, ok := next.(*sortedGroup)
	if !ok {
		g.next = nil
		return
	}
	g.next = n
}

func (g *sortedGroup) setPrev(prev link) {
	p, ok := prev.(*sortedGroup)
	if !ok {
		g.prev = nil
		return
	}
	g.prev = p
}

func (g *sortedGroup) remove() {
	unlink(g)
}

func (g *sortedGroup) addAfter(prev *sortedGroup) {
	addAfter(g, prev)
}

func (g *sortedGroup) addBefore(next *sortedGroup) {
	addBefore(g, next)
}

func (g *sortedGroup) insertAfter(prev *sortedGroup) {
	insertAfter(g, prev)
}

func (g *sortedGroup) insertBefore(next *sortedGroup) {
	insertBefore(g, next)
}

type sendable struct {
	sts.Hashed
	prev   string
	offset int64
	length int64
}

func (s *sendable) GetPrev() string {
	return s.prev
}

func (s *sendable) GetSlice() (int64, int64) {
	return s.offset, s.length
}

// Tag is the struct for defining the configuration for a set of groups.
type Tag struct {
	Name      string
	Priority  int
	Order     string
	ChunkSize int64
	LastDelay time.Duration
}

// Tagged implements sts.FileQueue for managing the ordering of file chunks
// based on a tagging approach where, based on path patterns, a tag is matched
// that applies certain behavior
type Tagged struct {
	byFile    map[string]*sortedFile
	byGroup   map[string]*sortedGroup
	headFile  map[string]*sortedFile
	headGroup *sortedGroup
	tags      []*Tag
	tagger    sts.Translate
	grouper   sts.Translate
	mux       sync.Mutex
}

// NewTagged creates a new tagged file queue
func NewTagged(tags []*Tag, tagger sts.Translate, grouper sts.Translate) *Tagged {
	q := &Tagged{
		tags:    tags,
		tagger:  tagger,
		grouper: grouper,
	}
	q.byFile = make(map[string]*sortedFile)
	q.byGroup = make(map[string]*sortedGroup)
	q.headFile = make(map[string]*sortedFile)
	return q
}

// Push adds a slice of hashed files to the queue
func (q *Tagged) Push(files []sts.Hashed) {
	q.mux.Lock()
	defer q.mux.Unlock()
	for _, file := range files {
		if orig, ok := q.byFile[file.GetName()]; ok {
			// If a file by this name is already here, let's start over
			q.removeFile(orig)
			orig.unlink()
		}
		groupName := q.grouper(file.GetName())
		group := q.getGroup(groupName)
		if group == nil {
			log.Info(fmt.Sprintf("Q: No matching tag found: %s",
				file.GetName()))
			continue
		}
		fileWrapper := &sortedFile{
			orig:  file,
			group: group,
		}
		_, isRecovered := file.(sts.Recovered)
		log.Debug(
			"Q Add:", file.GetName(),
			"Recovered?", isRecovered,
			"Allocated?", fileWrapper.isAllocated())
		q.addFile(fileWrapper)
	}
}

// Pop returns the next sendable chunk in the queue
func (q *Tagged) Pop() sts.Sendable {
	q.mux.Lock()
	defer q.mux.Unlock()
	g := q.headGroup
	var prev *sortedFile
	var next *sortedFile
	for g != nil {
		next = q.headFile[g.name]
		for next != nil && next.isAllocated() {
			if next.next == nil {
				// The head file should stay
				next = nil
				break
			}
			// Remove allocated files that may have been inserted simply for
			// getting files in the right order
			q.removeFile(next)
			prev = next.prev
			next = next.next
			if prev != nil {
				prev.unlink()
			}
		}
		if next == nil {
			g = g.next
			continue
		}
		if g.conf.LastDelay > 0 && next.next == nil {
			if time.Now().Sub(time.Unix(next.orig.GetTime(), 0)) < g.conf.LastDelay {
				// This is the last file in the group and it's not old enough
				// to send yet.
				next = nil
				g = g.next
				continue
			}
		}
		q.delayGroup(g)
		break
	}
	if next == nil {
		return nil
	}
	offset, length := next.allocate(g.conf.ChunkSize)
	chunk := &sendable{
		Hashed: next.orig,
		prev:   next.getPrevName(),
		offset: offset,
		length: length,
	}
	if chunk.prev == chunk.Hashed.GetName() {
		// It's possible to send a file by the same name again and we don't want
		// it to be dependent on itself
		chunk.prev = ""
	}
	log.Debug("Q Out:", chunk.GetName(), "<-", chunk.prev)
	if next.isAllocated() {
		log.Debug("Q Done:", next.orig.GetName(), offset, length, next.orig.GetSize())
		// File is fully allocated and can be removed from the Q
		q.removeFile(next)
		for next.prev != nil {
			// Unlink all previous files because they are no longer needed
			next.prev.unlink()
		}
		if q.headFile[next.group.name] == nil {
			// Put the head file back if it's nil--we want to keep this file's
			// place for the next one that comes in
			q.headFile[next.group.name] = next
			// If we get here then this file is just a placeholder and doesn't
			// need its predecessor
			next.unlink()
		}
	}
	return chunk
}

func (q *Tagged) delayGroup(group *sortedGroup) {
	p := group.conf.Priority
	n := group
	// Find last group of same priority
	for n.next != nil && n.next.conf.Priority == p {
		n = n.next
	}
	if n == group {
		// Either we only have one group of this priority or this group is
		// already the last one. Either way, we don't need to do anything.
		return
	}
	if group == q.headGroup {
		// Point to the next one if we're moving the head group
		q.headGroup = group.next
	}
	group.insertAfter(n)
}

func (q *Tagged) getGroup(groupName string) *sortedGroup {
	group, exists := q.byGroup[groupName]
	if !exists {
		group = &sortedGroup{
			name: groupName,
		}
		tagName := q.tagger(groupName)
		for _, tag := range q.tags {
			if tag.Name == tagName {
				group.conf = tag
				break
			}
		}
		if group.conf == nil {
			return nil
		}
		q.addGroup(group)
		q.byGroup[groupName] = group
	}
	return group
}

func (q *Tagged) addGroup(group *sortedGroup) {
	g := q.headGroup
	if g == nil {
		q.headGroup = group
		return
	}
	for g != nil {
		if group.conf.Priority > g.conf.Priority {
			group.addBefore(g)
			if g == q.headGroup {
				q.headGroup = group
			}
			break
		}
		if g.next == nil {
			group.addAfter(g)
			break
		}
		g = g.next
	}
}

func (q *Tagged) addFile(file *sortedFile) {
	q.byFile[file.orig.GetName()] = file
	head := q.headFile[file.group.name]
	if head == nil {
		q.headFile[file.group.name] = file
		return
	}
	f := head
	t1 := file.orig.GetTime()
	n1 := file.orig.GetName()
	order := file.group.conf.Order
loop:
	for f != nil {
		t0 := f.orig.GetTime()
		n0 := f.orig.GetName()
		switch {
		case order == sts.OrderAlpha || t0 == t1:
			if n1 < n0 {
				goto before
			}
		case order == sts.OrderFIFO:
			if t1 < t0 {
				goto before
			}
		case order == sts.OrderLIFO:
			if t1 > t0 {
				goto before
			}
		}
		if f.next == nil {
			goto after
		}
		f = f.next
		continue
	before:
		log.Debug("Q Insert:", file.orig.GetName(), "->", f.orig.GetName())
		file.insertBefore(f)
		break loop
	after:
		log.Debug("Q Insert:", file.orig.GetName(), "<-", f.orig.GetName())
		file.insertAfter(f)
		break loop
	}
	// Update the head file if we inserted before it or if we inserted after it
	// and it's already allocated
	if f == head && (f.prev == file || f.isAllocated()) {
		log.Debug("Q Head:", file.orig.GetName())
		q.headFile[file.group.name] = file
	}
}

func (q *Tagged) removeFile(file *sortedFile) {
	if q.headFile[file.group.name] == file {
		q.headFile[file.group.name] = file.next
	}
	delete(q.byFile, file.orig.GetName())
}
