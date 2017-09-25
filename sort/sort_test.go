package sort

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/conf"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/logging"
)

type mockScanFile struct {
	path string
	size int64
	time int64
}

func (f *mockScanFile) GetPath(follow bool) string {
	return f.path
}

func (f *mockScanFile) GetRelPath() string {
	return f.path
}

func (f *mockScanFile) GetSize() int64 {
	return f.size
}

func (f *mockScanFile) GetTime() int64 {
	return f.time
}

func (f *mockScanFile) GetHash() string {
	return fileutil.StringMD5(f.path)
}

func (f *mockScanFile) Reset() (bool, error) {
	return false, nil
}

type mockLogger struct {
}

func (log *mockLogger) Debug(params ...interface{}) {
	fmt.Println(params)
}

func (log *mockLogger) Info(params ...interface{}) {
	fmt.Println(params)
}

func (log *mockLogger) Error(params ...interface{}) {
	fmt.Println(params)
}

func (log *mockLogger) Sent(params ...interface{}) {
}

func (log *mockLogger) Received(params ...interface{}) {
}

func TestGeneral(t *testing.T) {
	logging.SetHandler(&mockLogger{})
	method := "test"
	tags := []*conf.Tag{
		&conf.Tag{
			Pattern:  regexp.MustCompile("^g1"),
			Method:   method,
			Order:    conf.OrderFIFO,
			Priority: 1,
		},
		&conf.Tag{
			Pattern:  regexp.MustCompile("^g2"),
			Method:   method,
			Order:    conf.OrderLIFO,
			Priority: 1,
		},
		&conf.Tag{
			Pattern:  regexp.MustCompile("^g3"),
			Method:   method,
			Order:    conf.OrderNone,
			Priority: 0,
		},
	}
	n := 10
	groupBy := regexp.MustCompile(`^([^\.]*)`)
	sorter := NewSorter(tags, groupBy)
	sorter.SetChunkSize(int64(n) * 10)
	inChan := make(chan []sts.ScanFile)
	outChan := make(chan sts.SortFile, n/10)
	var files []sts.ScanFile
	mt := time.Now().Unix()
	for i := n; i > 0; i-- {
		g := 1
		if i%3 == 0 {
			g = 3
		} else if i%2 == 0 {
			g = 2
			mt += int64(1)
		} else {
			mt -= int64(1)
		}
		files = append(files, &mockScanFile{
			path: fmt.Sprintf("g%d.f%02d", g, i),
			size: int64(i * 100),
			time: mt,
		})
	}
	go func(in chan []sts.ScanFile, out chan sts.SortFile, files []sts.ScanFile) {
		in <- files
		// Doing this will mess up the order but it's important that at least
		// files aren't getting stuck in the sorter.
		// for _, f := range files {
		// 	in <- []sts.ScanFile{f}
		// }
		close(in)
	}(inChan, outChan, files)
	go sorter.Start(inChan, map[string]chan sts.SortFile{method: outChan})
	var done []*sortedFile
	doneByGroup := make(map[string][]*sortedFile)
	for {
		f := <-outChan
		if !f.IsLast() {
			continue
		}
		done = append(done, f.(*sortedFile))
		g := strings.Split(f.GetRelPath(), ".")[0]
		doneByGroup[g] = append(doneByGroup[g], f.(*sortedFile))
		if len(done) == len(files) {
			break
		}
	}
	for g, gFiles := range doneByGroup {
		for i, f := range gFiles[1:] {
			switch g {
			case "g1":
				if f.GetPrevName() == "" {
					t.Error(f.GetRelPath(), "does not have a predessor")
				}
				if f.GetTime() < gFiles[i].GetTime() {
					t.Error(f.GetRelPath(), "should not follow", gFiles[i].GetRelPath())
				}
			case "g2":
				if f.GetPrevName() == "" {
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
	for _, f := range done {
		// g1 and g2 should alternate and g3 should be at the end if sorting is
		// done correctly.
		// TODO: this needs to be fixed now that slices of files can be sent
		// g := 1
		// if i >= len(done)-len(doneByGroup["g3"]) {
		// 	g = 3
		// } else if i%2 == 0 {
		// 	g = 2
		// }
		// if f.getGroup() != fmt.Sprintf("g%d", g) {
		// 	t.Error("File (", f.GetRelPath(), fmt.Sprintf(") should be g%d", g))
		// }
		// A file should be unlinked right after being sent out
		if f.getNext() != nil || f.getPrev() != nil {
			t.Error("File (", f.GetRelPath(), ") should be unlinked")
		}
	}
	for group, headFile := range sorter.head {
		if headFile != nil {
			t.Error("Group (", group, ") head file should be nil:", headFile.GetRelPath())
		}
	}
	if len(sorter.files) > 0 {
		t.Fatal("List of files should be empty")
	}
	// Block until the output channel is closed
	<-outChan
}
