package log

import (
	"fmt"
	"os"
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
	logger := NewReceive(root)
	n := 1000
	start := time.Now()
	var names []string
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		host := fmt.Sprintf("host%d", i%2)
		name := fmt.Sprintf("file/name-%d.ext", i)
		names = append(names, name)
		go func() {
			defer wg.Done()
			logger.Received(host, &mock.File{
				Name: name,
				Size: 1024 * 1024 * int64(i),
				Hash: fileutil.StringMD5(name),
			})
		}()
	}
	wg.Wait()
	for i, name := range names {
		host := fmt.Sprintf("host%d", i%2)
		if !logger.WasReceived(host, name, start, time.Now()) {
			t.Error("Failed to find:", name)
		}
	}
}
