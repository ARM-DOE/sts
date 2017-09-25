package send

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/units"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/logging"
	"code.arm.gov/dataflow/sts/server"
)

type mockSendable struct {
	path string
	size int64
	time int64
}

func (f *mockSendable) GetPath(follow bool) string {
	return f.path
}

func (f *mockSendable) GetRelPath() string {
	return f.path
}

func (f *mockSendable) GetSize() int64 {
	return f.size
}

func (f *mockSendable) GetTime() int64 {
	return f.time
}

func (f *mockSendable) GetHash() string {
	return fileutil.StringMD5(f.path)
}

func (f *mockSendable) GetSlice() (int64, int64) {
	return 0, f.GetSize()
}

func (f *mockSendable) GetPrevName() string {
	return ""
}

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

var sendName = "sender"
var recvName = "receiver"
var hostName = "localhost"
var hostPort = 1992
var root = "/var/tmp/sts-send"
var rootOut = filepath.Join(root, "out", recvName)
var rootStage = filepath.Join(root, "stage", sendName)
var rootFinal = filepath.Join(root, "final", sendName)

func tearDown() {
	os.RemoveAll(root)
}

func stageFiles(count int, bytes units.Base2Bytes) []string {
	os.MkdirAll(rootOut, os.ModePerm)
	var paths []string
	var b []byte
	for i := 0; i < count; i++ {
		f, err := ioutil.TempFile(rootOut, "example."+strconv.Itoa(i)+".")
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

	// Start receiver
	svrConf := &server.ReceiverConf{
		StageDir: rootStage,
		FinalDir: rootFinal,
		Host:     hostName,
		Port:     hostPort,
		Sources:  []string{"sender"},
	}
	server := server.NewReceiver(svrConf, nil)
	stop := make(chan bool)
	go server.Serve(nil, stop)

	// Start sender
	sndConf := &sts.SendConf{
		Threads:     3,
		Compression: 1,
		SourceName:  sendName,
		TargetName:  recvName,
		TargetHost:  fmt.Sprintf("%s:%d", hostName, hostPort),
		BinSize:     units.MiB * 10,
		Timeout:     time.Second * 2,
	}
	http, err := NewHTTP(sndConf)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(sndConf, http.SendBin)
	if err != nil {
		t.Fatal(err)
	}
	chIn := make(chan sts.Sendable, sndConf.Threads*2)
	chOut := make(chan []sts.Sent, sndConf.Threads*2)
	go sender.Start(chIn, chOut)

	// Pass in the files
	for _, f := range files {
		info, err := os.Lstat(f)
		if err != nil {
			t.Fatal(err)
		}
		chIn <- &mockSendable{
			path: f,
			size: info.Size(),
			time: info.ModTime().Unix(),
		}
	}

	// Wait for them to all send
	var done []sts.Sent
	for {
		done = append(done, <-chOut...)
		if len(done) == len(files) {
			break
		}
	}
	// Trigger the server to shutdown
	close(stop)
	// Trigger the sender to shutdown
	close(chIn)
	// Wait for the out channel to close
	<-chOut
}
