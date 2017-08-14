package scan

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/logging"

	"github.com/alecthomas/units"
)

type mockLogger struct {
	test *testing.T
}

func (log *mockLogger) Debug(params ...interface{}) {
	fmt.Println(params)
}

func (log *mockLogger) Info(params ...interface{}) {
	fmt.Println(params)
}

func (log *mockLogger) Error(params ...interface{}) {
	log.test.Error(params...)
}

func (log *mockLogger) Sent(params ...interface{}) {
}

func (log *mockLogger) Received(params ...interface{}) {
}

var root = "/var/tmp/sts-scan"
var rootScan = filepath.Join(root, "out", "receiver")

func tearDown() {
	os.RemoveAll(root)
}

func stageFiles(count int, bytes units.Base2Bytes) []string {
	os.MkdirAll(rootScan, os.ModePerm)
	var paths []string
	var b []byte
	for i := 0; i < count; i++ {
		f, err := ioutil.TempFile(rootScan, "example."+strconv.Itoa(i)+".")
		if err != nil {
			panic(err)
		}
		b = make([]byte, int(float64(bytes)*(float64(i+1)/float64(count))))
		_, err = rand.Read(b)
		if err != nil {
			panic(err)
		}
		f.Write(b)
		f.Close()
		paths = append(paths, f.Name())
	}
	return paths
}

func TestGeneral(t *testing.T) {
	logging.SetHandler(&mockLogger{test: t})

	// Stage some files
	tearDown()
	files := stageFiles(200, units.KiB*100)

	conf := &ScannerConf{
		ScanDir: rootScan,
	}

	scanner, err := NewScanner(conf)
	if err != nil {
		t.Error(err)
	}

	outCh := make(chan []sts.ScanFile, 1)
	doneCh := make(chan []sts.DoneFile, 1)

	go scanner.Start(outCh, doneCh, nil)

	scanned := <-outCh
	if len(scanned) != len(files) {
		t.Fatal("Mismatch")
	}
}
