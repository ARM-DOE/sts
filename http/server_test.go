package http

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
	"github.com/alecthomas/units"
)

var root = "/var/tmp/sts"
var rootStatic = filepath.Join(root, "serve", "sender")

func tearDown() {
	os.RemoveAll(root)
}

func stageFiles(count int, bytes units.Base2Bytes) {
	os.MkdirAll(rootStatic, os.ModePerm)
	b := make([]byte, bytes)
	for i := 0; i < count; i++ {
		f, err := ioutil.TempFile(rootStatic, "example."+strconv.Itoa(i)+".")
		if err != nil {
			panic(err)
		}
		_, err = rand.Read(b)
		if err != nil {
			panic(err)
		}
		f.Write(b)
		f.Close()
	}
}

func request(method, url string, data io.Reader, respData ...interface{}) error {
	var err error
	var client *http.Client
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser

	if client, err = GetClient(nil); err != nil {
		return err
	}

	if req, err = http.NewRequest(method, "http://localhost:1992"+url, data); err != nil {
		return err
	}
	req.Header.Add(HeaderSourceName, "sender")

	if resp, err = client.Do(req); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}

	if len(respData) == 0 {
		return nil
	}

	if reader, err = GetRespReader(resp); err != nil {
		return err
	}
	defer reader.Close()

	switch resp.Header.Get(HeaderContentType) {
	case HeaderJSON:
		jsonDecoder := json.NewDecoder(reader)
		err = jsonDecoder.Decode(respData[0])
		if err != nil {
			return err
		}
	default:
		respData[0].(*bytes.Buffer).ReadFrom(reader)
	}
	return nil
}

func TestCRUD(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})

	fsize := units.KiB
	tearDown()
	stageFiles(10, fsize)

	server := &Server{
		ServeDir: filepath.Join(root, "serve"),
		Host:     "localhost",
		Port:     1992,
	}
	stop := make(chan bool)
	done := make(chan bool)
	go server.Serve(stop, done)

	names := []string{}
	if err := request(http.MethodGet, "/static", nil, &names); err != nil {
		t.Fatal(err)
	}
	if len(names) != 10 {
		t.Fatal(names)
	}

	for _, n := range names {
		data := bytes.Buffer{}
		if err := request(http.MethodGet, "/static/"+n, nil, &data); err != nil {
			t.Fatal(err)
		}
		if data.Len() != int(fsize) {
			t.Fatal(data.Len())
		}
		if err := request(http.MethodDelete, "/static/"+n, nil); err != nil {
			t.Fatal(err)
		}
	}
	stop <- true
	<-done
}
