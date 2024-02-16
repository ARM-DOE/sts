package http

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
	"github.com/alecthomas/units"
)

var port = 1992
var source = "sender"
var root = "/var/tmp/sts"
var rootStatic = filepath.Join(root, "serve", source)

func tearDown() {
	os.RemoveAll(root)
}

func stageFiles(count int, bytes units.Base2Bytes) {
	os.MkdirAll(rootStatic, os.ModePerm)
	b := make([]byte, bytes)
	for i := 0; i < count; i++ {
		f, err := os.CreateTemp(rootStatic, "example."+strconv.Itoa(i)+".")
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

func requestInternal(url string) error {
	var err error
	var client *http.Client
	var req *http.Request
	var resp *http.Response

	if client, err = GetClient(nil); err != nil {
		return err
	}

	if req, err = http.NewRequest(
		http.MethodPut, fmt.Sprintf("http://localhost:%d%s", port+1, url), nil,
	); err != nil {
		return err
	}
	req.Header.Add(HeaderSourceName, source)

	if resp, err = client.Do(req); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return nil
}

func request(method, url string, data io.Reader, respData ...any) error {
	var err error
	var client *http.Client
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser

	if client, err = GetClient(nil); err != nil {
		return err
	}

	if req, err = http.NewRequest(
		method, fmt.Sprintf("http://localhost:%d%s", port, url), data,
	); err != nil {
		return err
	}
	req.Header.Add(HeaderSourceName, source)

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

func TestMisc(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})

	fsize := units.KiB
	tearDown()
	stageFiles(10, fsize)

	gk := &mockGk{}
	server := &Server{
		ServeDir: filepath.Join(root, "serve"),
		Host:     "localhost",
		Port:     port,
		IsValid: func(source, key string) bool {
			return true
		},
		GateKeepers: map[string]sts.GateKeeper{source: gk},
		GateKeeperFactory: func(name string) sts.GateKeeper {
			return nil
		},
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

	payload := map[string]any{
		"path":   `C:\Data\2024\133.dat`,
		"source": source,
		"root":   `C:\Data`,
		"mapping": []any{
			map[string]string{
				"from": `^(?P<year>\d{4})\\(?P<day>\d+)\.dat$`,
				"to":   `{{.__source}}.{{parseDayOfYear .year .day | formatDate "Ymd"}}.000000.dat`,
			},
		},
	}

	marshaled, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	parsedResp := make(map[string]string)

	if err := request(http.MethodPost, "/check-mapping", bytes.NewReader(marshaled), &parsedResp); err != nil {
		t.Fatal(err)
	}

	if parsedResp["mapped"] != fmt.Sprintf("%s.20240512.000000.dat", source) {
		t.Fatal(parsedResp)
	}

	if err := requestInternal("/clean?block"); err != nil {
		t.Fatal(err)
	}

	if !gk.gotCleanedNow {
		t.Fatal("not cleaned")
	}

	if err := requestInternal("/restart?block"); err != nil {
		t.Fatal(err)
	}

	if !gk.gotStopped || !gk.gotRecovered {
		t.Fatal("not restarted")
	}

	if err := requestInternal("/debug"); err != nil {
		t.Fatal(err)
	}

	stop <- true
	<-done
}

type mockGk struct {
	gotCleanedNow bool
	gotStopped    bool
	gotRecovered  bool
}

func (g *mockGk) Recover() {
	g.gotRecovered = true
}

func (g *mockGk) CleanNow(t time.Duration) {
	g.gotCleanedNow = true
}

func (g *mockGk) Ready() bool {
	return true
}

func (g *mockGk) Scan(version string) (json []byte, err error) {
	return
}

func (g *mockGk) Prepare(request []sts.Binned) {
	//
}

func (g *mockGk) Receive(file *sts.Partial, reader io.Reader) (err error) {
	return
}

func (g *mockGk) Received(parts []sts.Binned) (nRecvd int) {
	return
}

func (g *mockGk) GetFileStatus(relPath string, sent time.Time) int {
	return sts.ConfirmNone
}

func (g *mockGk) Stop(force bool) {
	g.gotStopped = true
	g.gotRecovered = false
}
