package server

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"code.arm.gov/dataflow/sts/httputil"
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

	if client, err = httputil.GetClient(nil); err != nil {
		return err
	}

	if req, err = http.NewRequest(method, "http://localhost:1992"+url, data); err != nil {
		return err
	}
	req.Header.Add(httputil.HeaderSourceName, "sender")

	if resp, err = client.Do(req); err != nil {
		return err
	}

	if len(respData) == 0 {
		return nil
	}

	if reader, err = httputil.GetRespReader(resp); err != nil {
		return err
	}
	defer reader.Close()

	switch resp.Header.Get(httputil.HeaderContentType) {
	case httputil.HeaderJSON:
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
	fsize := units.KiB
	tearDown()
	stageFiles(10, fsize)

	conf := &ReceiverConf{
		ServeDir: filepath.Join(root, "serve"),
		Port:     1992,
		Sources:  []string{"sender"},
	}
	server := NewReceiver(conf, nil)
	stop := make(chan bool)
	go server.Serve(nil, stop)

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
}
