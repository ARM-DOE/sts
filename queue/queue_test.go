package queue

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
)

func TestGeneral(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	tags := []*Tag{
		&Tag{
			Name:     "^g1",
			Order:    ByFIFO,
			Priority: 1,
		},
		&Tag{
			Name:     "^g2",
			Order:    ByLIFO,
			Priority: 1,
		},
		&Tag{
			Name:     "^g3",
			Order:    ByAlpha,
			Priority: 0,
		},
	}
	n := 1000
	groupBy := regexp.MustCompile(`^([^\.]*)`)
	grouper := func(name string) (group string) {
		m := groupBy.FindStringSubmatch(name)
		if len(m) == 0 {
			return
		}
		group = m[1]
		return
	}
	tagger := func(group string) (tag string) {
		for _, t := range tags {
			if regexp.MustCompile(t.Name).MatchString(group) {
				tag = t.Name
				return
			}
		}
		return
	}
	queue := NewTagged(tags, tagger, grouper)
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
	queue.Push(files)
	var done []*sendable
	doneByGroup := make(map[string][]*sendable)
	for {
		f := queue.Pop()
		done = append(done, f.(*sendable))
		g := strings.Split(f.GetRelPath(), ".")[0]
		doneByGroup[g] = append(doneByGroup[g], f.(*sendable))
		if len(done) == len(files) {
			break
		}
	}
	for g, gFiles := range doneByGroup {
		for i, f := range gFiles[1:] {
			switch g {
			case "g1":
				if f.GetPrev() == "" {
					t.Error(f.GetRelPath(), "does not have a predessor")
				}
				if f.GetTime() < gFiles[i].GetTime() {
					t.Error(f.GetRelPath(), "should not follow", gFiles[i].GetRelPath())
				}
			case "g2":
				if f.GetPrev() == "" {
					t.Error(f.GetRelPath(), "does not have a predessor")
				}
				// g2 and g3 are the same given the original LIFO order of
				// the array
				fallthrough
			case "g3":
				if f.GetTime() > gFiles[i].GetTime() {
					t.Error(f.GetRelPath(), "should not follow", gFiles[i].GetRelPath())
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
		if grouper(f.GetRelPath()) != fmt.Sprintf("g%d", g) {
			t.Error("File (", f.GetRelPath(), fmt.Sprintf(") should be g%d", g))
		}
	}
	for group, headFile := range queue.headFile {
		if headFile != nil {
			t.Error("Group (", group, ") head file should be nil:", headFile.orig.GetRelPath())
		}
	}
	if len(queue.byFile) > 0 {
		t.Fatal("List of files should be empty")
	}
}
