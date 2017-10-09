package queue

import (
	"fmt"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
)

const (
	// ByFIFO indicates first in, first out ordering
	ByFIFO = "fifo"

	// ByLIFO indicates last in, first out ordering
	ByLIFO = "lifo"

	// ByAlpha indicates an alphabetic ordering
	ByAlpha = ""
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
	if prev != nil {
		prev.setNext(next)
		node.setPrev(nil)
	}
	if next != nil {
		next.setPrev(prev)
		node.setNext(nil)
	}
}

func addAfter(node link, prev link) {
	node.setNext(prev.getNext())
	node.setPrev(prev)
	prev.setNext(node)
	if node.getNext() != nil {
		node.getNext().setPrev(node)
	}
}

func addBefore(node link, next link) {
	node.setNext(next)
	node.setPrev(next.getPrev())
	next.setPrev(node)
	if node.getPrev() != nil {
		node.getPrev().setNext(node)
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
	return f.prev.orig.GetRelPath()
}

func (f *sortedFile) setNext(next link) {
	f.next = next.(*sortedFile)
}

func (f *sortedFile) setPrev(prev link) {
	f.prev = prev.(*sortedFile)
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

func newSortedGroup(name string, conf *Tag) *sortedGroup {
	g := &sortedGroup{}
	g.name = name
	g.conf = conf
	return g
}

func (g *sortedGroup) getNext() link {
	return g.next
}

func (g *sortedGroup) getPrev() link {
	return g.prev
}

func (g *sortedGroup) setNext(next link) {
	g.next = next.(*sortedGroup)
}

func (g *sortedGroup) setPrev(prev link) {
	g.prev = prev.(*sortedGroup)
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
	orig   sts.Hashed
	prev   string
	offset int64
	length int64
}

func (s *sendable) GetPath() string {
	return s.orig.GetPath()
}

func (s *sendable) GetRelPath() string {
	return s.orig.GetRelPath()
}

func (s *sendable) GetTime() int64 {
	return s.orig.GetTime()
}

func (s *sendable) GetSize() int64 {
	return s.orig.GetSize()
}

func (s *sendable) GetHash() string {
	return s.orig.GetHash()
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

// Add adds a slice of hashed files to the queue
func (q *Tagged) Add(files []sts.Hashed) {
	for _, file := range files {
		if orig, ok := q.byFile[file.GetRelPath()]; ok {
			// If a file by this name is already here, let's start over
			q.removeFile(orig)
		}
		groupName := q.grouper(file.GetRelPath())
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
				log.Debug(fmt.Sprintf("Q: No matching tag found: %s", file.GetRelPath()))
				continue
			}
			q.addGroup(group)
		}
		fileWrapper := &sortedFile{
			orig:  file,
			group: group,
		}
		q.addFile(fileWrapper)
	}
}

// GetNext returns the next sendable chunk in the queue
func (q *Tagged) GetNext() sts.Sendable {
	g := q.headGroup
	var next *sortedFile
	for g != nil {
		next = q.headFile[g.name]
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
		orig:   next.orig,
		prev:   next.prev.orig.GetRelPath(),
		offset: offset,
		length: length,
	}
	if next.isAllocated() {
		// File is fully allocated and can be removed from the queue
		q.removeFile(next)
	}
	return chunk
}

// Queued returns whether or not the file is in the queue
func (q *Tagged) Queued(relPath string) bool {
	_, ok := q.byFile[relPath]
	return ok
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
	q.byFile[file.orig.GetRelPath()] = file
	f := q.headFile[file.group.name]
	if f == nil {
		q.headFile[file.group.name] = file
		return
	}
	n := 0
loop:
	for f != nil {
		switch file.group.conf.Order {
		case ByFIFO:
			if file.orig.GetTime() < f.orig.GetTime() {
				file.insertBefore(f)
				break loop
			}
			if file.orig.GetTime() == f.orig.GetTime() {
				if file.orig.GetRelPath() < f.orig.GetRelPath() {
					file.insertBefore(f)
					break loop
				}
			}
		case ByLIFO:
			if file.orig.GetTime() > f.orig.GetTime() {
				file.insertBefore(f)
				break loop
			}
			if file.orig.GetTime() == f.orig.GetTime() {
				if file.orig.GetRelPath() > f.orig.GetRelPath() {
					file.insertBefore(f)
					break loop
				}
			}
		case ByAlpha:
			if file.orig.GetRelPath() < f.orig.GetRelPath() {
				file.insertBefore(f)
				break loop
			}
		}
		n++
		if f.next == nil {
			file.insertAfter(f)
			break
		}
		f = f.next
	}
	// If we inserted before the head file we need to update the pointer.
	if n == 0 {
		q.headFile[file.group.name] = file
	}
}

func (q *Tagged) removeFile(file *sortedFile) {
	if q.headFile[file.group.name] == file {
		q.headFile[file.group.name] = file.next
	}
	if file.prev != nil {
		file.prev.unlink()
	}
	if file.next != nil {
		file.unlink()
	}
	delete(q.byFile, file.orig.GetRelPath())
}
