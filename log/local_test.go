package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/mock"
)

var root = "/var/tmp/sts-log"

func tearDown() {
	os.RemoveAll(root)
}

func TestDefault(t *testing.T) {
	tearDown()
	Init(root, true)
	Debug("This is a debug log message")
	Info("This is an info log message")
	Error("This is an error log message")
}

func TestSend(t *testing.T) {
	tearDown()
	logger := NewSend(root)
	n := 1000
	start := time.Now()
	var names []string
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("file/name-%d.ext", i)
		names = append(names, name)
		go func() {
			defer wg.Done()
			logger.Sent(&mock.File{
				Name:     name,
				Size:     1024 * 1024 * int64(i),
				Hash:     fileutil.StringMD5(name),
				SendTime: int64(i),
			})
		}()
	}
	wg.Wait()
	for _, name := range names {
		if !logger.WasSent(name, start, time.Now()) {
			t.Error("Failed to find:", name)
		}
	}
}

func TestReceive(t *testing.T) {
	tearDown()
	n := 1000
	start := time.Now()
	byHost := make(map[string]*Receive)
	var names []string
	var wg sync.WaitGroup
	var logger *Receive
	var host string
	wg.Add(n)
	for i := 0; i < n; i++ {
		host = fmt.Sprintf("host%d", i%2)
		if logger = byHost[host]; logger == nil {
			logger = NewReceive(filepath.Join(root, host))
			byHost[host] = logger
		}
		name := fmt.Sprintf("file/name-%d.ext", i)
		names = append(names, name)
		go func(lh *Receive) {
			defer wg.Done()
			lh.Received(&mock.File{
				Name: name,
				Size: 1024 * 1024 * int64(i),
				Hash: fileutil.StringMD5(name),
			})
		}(logger)
	}
	wg.Wait()
	for i, name := range names {
		host = fmt.Sprintf("host%d", i%2)
		if !byHost[host].WasReceived(name, start, time.Now()) {
			t.Error("Failed to find:", name)
		}
	}
	parsed := 0
	for host, logger = range byHost {
		logger.Parse(func(name, hash string, size int64, r time.Time) bool {
			parsed++
			if size == 0 || r.IsZero() {
				t.Error("Bad size/time:", size, r)
			}
			println(host, name, hash, size, r.String())
			return false
		}, start, time.Now())
	}
	if parsed < len(names) {
		t.Fatal("Failed to parse each log line")
	}
}
