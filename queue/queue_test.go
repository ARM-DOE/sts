package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/marshal"
	"github.com/arm-doe/sts/mock"
)

var (
	groupBy *regexp.Regexp
	grouper sts.Translate
	tagger  sts.Translate
)

func q(tags []*Tag, groupByOverride *regexp.Regexp) *Tagged {
	if groupByOverride == nil {
		groupBy = regexp.MustCompile(`^([^\.]*)`)
	} else {
		groupBy = groupByOverride
	}
	tagger = func(group string) (tag string) {
		for _, t := range tags {
			if t.Name == group || regexp.MustCompile(t.Name).MatchString(group) {
				tag = t.Name
				return
			}
		}
		return
	}
	grouper = func(name string) (group string) {
		m := groupBy.FindStringSubmatch(name)
		if len(m) > 1 {
			group = m[1]
			if group != "" && group != name {
				return
			}
		}
		// If the group matches the name or is empty, use the tag as the group
		group = tagger(name)
		return
	}
	return NewTagged(tags, tagger, grouper)
}

func TestRecover(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	tags := []*Tag{
		{
			Name:  "^g1",
			Order: sts.OrderFIFO,
		},
	}
	q := q(tags, nil)
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

func TestPrev(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	tags := []*Tag{
		{
			Name:     "",
			Order:    sts.OrderFIFO,
			Priority: 0,
		},
	}
	n := 10000
	g := "^/?([^./]*)"
	queue := q(tags, regexp.MustCompile(g))
	var files []sts.Hashed
	for i := n; i > 0; i-- {
		mt := time.Now().Add(time.Hour * time.Duration(-i))
		yyyy := mt.Format("2006")
		mm := mt.Format("01")
		dd := mt.Format("02")
		hh := mt.Format("15")
		name := fmt.Sprintf(
			"%s/%s%s/%s%s%s/Stare_235_%s%s%s_%s_%d.hpl",
			yyyy,
			yyyy, mm,
			yyyy, mm, dd,
			yyyy, mm, dd, hh, i,
		)
		files = append(files, &mock.File{
			Path: name,
			Name: name,
			Size: int64(i * 100),
			Time: mt,
		})
	}
	queue.Push(files)
	var done []*sendable
	for {
		f := queue.Pop()
		if f == nil {
			t.Fatal("Popped file should not be nil", len(done))
		}
		if f.GetPrev() == f.GetName() {
			t.Fatal("A file should not depend on itself")
		}
		done = append(done, f.(*sendable))
		if len(done) == len(files) {
			break
		}
	}
	done = nil
	queue.Push([]sts.Hashed{files[len(files)-1]})
	f := queue.Pop()
	if f == nil {
		t.Fatal("Popped file should not be nil", len(done))
	}
	if f.GetPrev() == f.GetName() {
		t.Fatal("A file should not depend on itself")
	}
}

func TestGeneral(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	tags := []*Tag{
		{
			Name:     "^g1",
			Order:    sts.OrderFIFO,
			Priority: 1,
		},
		{
			Name:     "^g2",
			Order:    sts.OrderLIFO,
			Priority: 1,
		},
		{
			Name:     "^g3",
			Order:    sts.OrderAlpha,
			Priority: 0,
		},
		{
			Name:     "^g4",
			Order:    sts.OrderNone,
			Priority: -1,
		},
	}
	n := 10000
	queue := q(tags, groupBy)
	var files []sts.Hashed
	for i := n; i > 0; i-- {
		g := 1
		mt := time.Now()
		if i%3 == 0 {
			g = 3
			if i%5 == 0 {
				g = 4
			}
		} else if i%2 == 0 {
			g = 2
			mt = mt.Add(time.Minute * time.Duration(i))
		} else {
			mt = mt.Add(time.Minute * time.Duration(-i))
		}
		name := fmt.Sprintf("g%d.f%05d", g, i)
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
				if f.GetTime().Before(gFiles[i].GetTime()) {
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
				if f.GetTime().After(gFiles[i].GetTime()) {
					t.Error(f.GetName(), "should not follow", gFiles[i].GetName())
				}
			case "g4":
				if f.GetPrev() != "" {
					t.Error(f.GetName(), "has a predessor but shouldn't")
				}
			}
		}
	}
	for i, f := range done {
		// g1 and g2 should alternate with g3 followed by g4 at the end if
		// sorting is done correctly.
		g := 1
		if i >= len(done)-len(doneByGroup["g4"]) {
			g = 4
		} else if i >= len(done)-len(doneByGroup["g4"])-len(doneByGroup["g3"]) {
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
			t.Errorf("Group (%s) head file should not be nil", group)
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
	group := "g2"
	nextName := fmt.Sprintf("%s.f%02d", group, n)
	prevOne := queue.headFile[group]
	lastOne := &mock.File{
		Path: nextName,
		Name: nextName,
		Size: int64(100),
		Time: time.Now(),
	}
	queue.Push([]sts.Hashed{lastOne})
	headFile := queue.headFile[group]
	if headFile == nil {
		t.Fatalf("Group (%s) head file should not be nil", group)
		return
	}
	if headFile.orig != lastOne {
		t.Fatal("List reset failed")
	}
	if headFile.prev != prevOne {
		t.Fatal("Invalid order following reset")
	}
	if headFile.next != nil {
		t.Fatal("Expected reset to have only one file")
	}
}

func TestGroupEqualsTag(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	tags := []*Tag{
		{
			Name:     "^g1",
			Order:    sts.OrderFIFO,
			Priority: 1,
		},
		{
			Name:     "^g2",
			Order:    sts.OrderLIFO,
			Priority: 1,
		},
		{
			Name:     "^g3",
			Order:    sts.OrderAlpha,
			Priority: 0,
		},
		{
			Name:     "^g4",
			Order:    sts.OrderNone,
			Priority: -1,
		},
	}
	n := 10000
	queue := q(tags, regexp.MustCompile(`.`))
	var files []sts.Hashed
	for i := n; i > 0; i-- {
		g := 1
		mt := time.Now()
		if i%3 == 0 {
			g = 3
			if i%5 == 0 {
				g = 4
			}
		} else if i%2 == 0 {
			g = 2
			mt = mt.Add(time.Minute * time.Duration(i))
		} else {
			mt = mt.Add(time.Minute * time.Duration(-i))
		}
		name := fmt.Sprintf("g%d.f%05d", g, i)
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
		g := fmt.Sprintf("^%s", strings.Split(f.GetName(), ".")[0])
		doneByGroup[g] = append(doneByGroup[g], f.(*sendable))
		if len(done) == n {
			break
		}
	}
	for g, gFiles := range doneByGroup {
		for i, f := range gFiles[1:] {
			switch g {
			case "^g1":
				if f.GetPrev() == "" {
					t.Error(f.GetName(), "does not have a predessor")
				}
				if f.GetTime().Before(gFiles[i].GetTime()) {
					t.Error(f.GetName(), "should not follow", gFiles[i].GetName())
				}
			case "^g2":
				if f.GetPrev() == "" {
					t.Error(f.GetName(), "does not have a predessor")
				}
				// g2 and g3 are the same given the original LIFO order of
				// the array
				fallthrough
			case "^g3":
				if f.GetTime().After(gFiles[i].GetTime()) {
					t.Error(f.GetName(), "should not follow", gFiles[i].GetName())
				}
			case "^g4":
				if f.GetPrev() != "" {
					t.Error(f.GetName(), "has a predessor but shouldn't")
				}
			}
		}
	}
	for i, f := range done {
		// g1 and g2 should alternate with g3 followed by g4 at the end if
		// sorting is done correctly.
		g := 1
		if i >= len(done)-len(doneByGroup["^g4"]) {
			g = 4
		} else if i >= len(done)-len(doneByGroup["^g4"])-len(doneByGroup["^g3"]) {
			g = 3
		} else if i%2 == 0 {
			g = 2
		}
		if grouper(f.GetName()) != fmt.Sprintf("^g%d", g) {
			t.Error("File (", f.GetName(), fmt.Sprintf(") should be ^g%d, not %s",
				g, grouper(f.GetName())))
		}
	}
	for group, headFile := range queue.headFile {
		if headFile == nil {
			t.Errorf("Group (%s) head file should not be nil", group)
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
	group := "^g2"
	nextName := fmt.Sprintf("%s.f%02d", strings.TrimLeft(group, "^"), n)
	prevOne := queue.headFile[group]
	lastOne := &mock.File{
		Path: nextName,
		Name: nextName,
		Size: int64(100),
		Time: time.Now(),
	}
	queue.Push([]sts.Hashed{lastOne})
	headFile := queue.headFile[group]
	if headFile == nil {
		t.Fatalf("Group (%s) head file should not be nil", group)
		return
	}
	if headFile.orig != lastOne {
		t.Fatal("List reset failed")
	}
	if headFile.prev != prevOne {
		t.Fatal("Invalid order following reset")
	}
	if headFile.next != nil {
		t.Fatal("Expected reset to have only one file")
	}
}

const (
	CachePath = ""
)

func TestJSONFile(t *testing.T) {
	if CachePath == "" {
		t.Skip("CachePath not set")
	}

	log.InitExternal(&mock.Logger{})
	tags := []*Tag{
		{
			Order: sts.OrderFIFO,
		},
	}
	queue := q(tags, nil)

	// Read JSON file
	data, err := os.ReadFile(CachePath)
	if err != nil {
		t.Fatalf("Failed to read JSON file: %v", err)
	}

	// Unmarshal JSON data
	var cache struct {
		Files map[string]struct {
			Size int64            `json:"size"`
			Time marshal.NanoTime `json:"mtime"`
			Hash string           `json:"hash"`
		} `json:"files"`
	}
	if err := json.Unmarshal(data, &cache); err != nil {
		t.Fatalf("Failed to unmarshal JSON data: %v", err)
	}

	// Convert to sts.Hashed
	var hashedFiles []sts.Hashed
	for name, f := range cache.Files {
		hashedFiles = append(hashedFiles, &mock.File{
			Path: name,
			Name: name,
			Size: f.Size,
			Time: f.Time.Time,
		})
	}

	// byName := make(map[string]sts.Hashed)
	// for _, f := range hashedFiles {
	//     byName[f.GetName()] = f
	// }

	// Push files to queue
	queue.Push(hashedFiles)

	// Pop files and check order
	prevToNext := make(map[string]sts.Sendable)
	files := make(map[string][]sts.Sendable)
	filesCtl := make(map[string][]sts.Sendable)
	for {
		f := queue.Pop()
		if f == nil {
			break
		}
		g := groupBy.FindStringSubmatch(f.GetName())[1]
		filesCtl[g] = append(filesCtl[g], f)
		p := f.GetPrev()
		if p == "" {
			if len(files[g]) > 0 {
				t.Errorf("File '%s' has no predecessor", f.GetName())
				continue
			}
			files[g] = append(files[g], f)
			continue
		}
		if _, ok := prevToNext[p]; ok {
			t.Errorf("File '%s' has multiple successors", p)
			continue
		}
		prevToNext[p] = f
	}
	for g := range files {
		f := files[g][0]
		for {
			next, ok := prevToNext[f.GetName()]
			if !ok {
				break
			}
			files[g] = append(files[g], next)
			f = next
		}
		n0 := files[g]
		n1 := filesCtl[g]
		var names0 []string
		for _, f := range n0 {
			names0 = append(names0, f.GetName())
		}
		names0str := strings.Join(names0, ":")
		sort.Slice(n1, func(i, j int) bool {
			return n1[i].GetTime().Before(n1[j].GetTime())
		})
		var names1 []string
		for _, f := range n1 {
			names1 = append(names1, f.GetName())
		}
		names1str := strings.Join(names1, ":")
		if names0str != names1str {
			t.Errorf("Incorrect order for group %s", g)
		}
		t.Logf("Group %s: %d", g, len(names0))
	}
}
