package queue

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
)

var (
	groupBy *regexp.Regexp
	grouper sts.Translate
	tagger  sts.Translate
)

func q(tags []*Tag) *Tagged {
	groupBy = regexp.MustCompile(`^([^\.]*)`)
	grouper = func(name string) (group string) {
		m := groupBy.FindStringSubmatch(name)
		if len(m) == 0 {
			return
		}
		group = m[1]
		return
	}
	tagger = func(group string) (tag string) {
		for _, t := range tags {
			if regexp.MustCompile(t.Name).MatchString(group) {
				tag = t.Name
				return
			}
		}
		return
	}
	return NewTagged(tags, tagger, grouper)
}

func TestRecover(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	tags := []*Tag{
		&Tag{
			Name:  "^g1",
			Order: sts.OrderFIFO,
		},
	}
	q := q(tags)
	now := time.Now()
	files := []sts.Hashed{
		&mock.RecoveredFile{
			Hashed: &mock.File{
				Name: "g1.f20",
				Size: 10,
				Time: now,
			},
		},
		&mock.File{
			Name: "g1.f04",
			Size: 10,
			Time: now,
		},
		&mock.File{
			Name: "g1.f07",
			Size: 10,
			Time: now,
		},
		&mock.File{
			Name: "g1.f05",
			Size: 10,
			Time: now,
		},
		&mock.File{
			Name: "g1.f06",
			Size: 10,
			Time: now,
		},
		&mock.RecoveredFile{
			Hashed: &mock.File{
				Name: "g1.f03",
				Size: 10,
				Time: now,
			},
		},
		&mock.File{
			Name: "g1.f01",
			Size: 10,
			Time: now,
		},
		&mock.File{
			Name: "g1.f02",
			Size: 10,
			Time: now,
		},
	}
	q.Push(files)
	var names []string
	for {
		f := q.Pop()
		if f == nil {
			break
		}
		names = append(names, f.GetName())
	}
	s1 := strings.Join(names, ":")
	sort.Strings(names)
	s2 := strings.Join(names, ":")
	if s1 != s2 {
		t.Fatal("Incorrect order")
	}
}

func TestGeneral(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	tags := []*Tag{
		&Tag{
			Name:     "^g1",
			Order:    sts.OrderFIFO,
			Priority: 1,
		},
		&Tag{
			Name:     "^g2",
			Order:    sts.OrderLIFO,
			Priority: 1,
		},
		&Tag{
			Name:     "^g3",
			Order:    sts.OrderAlpha,
			Priority: 0,
		},
	}
	n := 1000
	queue := q(tags)
	var files []sts.Hashed
	for i := n; i > 0; i-- {
		g := 1
		mt := time.Now()
		if i%3 == 0 {
			g = 3
		} else if i%2 == 0 {
			g = 2
			mt = mt.Add(time.Minute * time.Duration(i))
		} else {
			mt = mt.Add(time.Minute * time.Duration(-i))
		}
		name := fmt.Sprintf("g%d.f%02d", g, i)
		files = append(files, &mock.File{
			Path: name,
			Name: name,
			Size: int64(i * 100),
			Time: mt,
		})
	}
	// Add them twice to make sure file replacement works
	queue.Push(files)
	queue.Push(files)
	var done []*sendable
	doneByGroup := make(map[string][]*sendable)
	for {
		f := queue.Pop()
		if f == nil {
			t.Fatal("Popped file should not be nil")
		}
		done = append(done, f.(*sendable))
		g := strings.Split(f.GetName(), ".")[0]
		doneByGroup[g] = append(doneByGroup[g], f.(*sendable))
		if len(done) == n {
			break
		}
	}
	for g, gFiles := range doneByGroup {
		for i, f := range gFiles[1:] {
			switch g {
			case "g1":
				if f.GetPrev() == "" {
					t.Error(f.GetName(), "does not have a predessor")
				}
				if f.GetTime() < gFiles[i].GetTime() {
					t.Error(f.GetName(), "should not follow", gFiles[i].GetName())
				}
			case "g2":
				if f.GetPrev() == "" {
					t.Error(f.GetName(), "does not have a predessor")
				}
				// g2 and g3 are the same given the original LIFO order of
				// the array
				fallthrough
			case "g3":
				if f.GetTime() > gFiles[i].GetTime() {
					t.Error(f.GetName(), "should not follow", gFiles[i].GetName())
				}
			}
		}
	}
	for i, f := range done {
		// g1 and g2 should alternate and g3 should be at the end if sorting is
		// done correctly.
		g := 1
		if i >= len(done)-len(doneByGroup["g3"]) {
			g = 3
		} else if i%2 == 0 {
			g = 2
		}
		if grouper(f.GetName()) != fmt.Sprintf("g%d", g) {
			t.Error("File (", f.GetName(), fmt.Sprintf(") should be g%d", g))
		}
	}
	for group, headFile := range queue.headFile {
		if headFile == nil {
			t.Errorf("Group (%s) head file should not be nil: %s",
				group,
				headFile.orig.GetName())
			continue
		}
		if !headFile.isAllocated() {
			t.Errorf("Group (%s) head file should have been allocated: %s",
				group,
				headFile.orig.GetName())
		}
		if headFile.prev != nil {
			t.Errorf("Group (%s) head file should not have a predecessor: %s",
				group,
				headFile.prev.orig.GetName())
		}
		if headFile.next != nil {
			t.Errorf("Group (%s) head file should be the last one", group)
		}
	}
	if len(queue.byFile) > 0 {
		t.Fatal("List of files should be empty")
	}
}
