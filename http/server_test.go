package http

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/mock"
)

var port = 1992
var source = "sender"
var root = "/var/tmp/sts"
var rootStatic = filepath.Join(root, "serve", source)

func tearDown() {
	os.RemoveAll(root)
}

func stageFiles(count int, bytes units.Base2Bytes) {
	_ = os.MkdirAll(rootStatic, os.ModePerm)
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
		_, _ = f.Write(b)
		f.Close()
	}
}

func requestInternal(url string) error {
	var err error
	var client *BandwidthLoggingClient
	var req *http.Request
	var resp *http.Response

	if client, err = GetClient(nil, time.Duration(0), ""); err != nil {
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
	var client *BandwidthLoggingClient
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser

	if client, err = GetClient(nil, time.Duration(0), ""); err != nil {
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
		_, _ = respData[0].(*bytes.Buffer).ReadFrom(reader)
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
		"root":   `C:\`,
		"mapping": []any{
			map[string]string{
				"from": `(?P<year>\d{4})\\(?P<day>\d+)\.dat$`,
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

func TestNormalizeRepeatedSlashes(t *testing.T) {
	tests := map[string]string{
		"":                     "/",
		"/sts//data":           "/sts/data",
		"//sts///data-recovery": "/sts/data-recovery",
		"/sts/data":            "/sts/data",
	}

	for input, expected := range tests {
		if got := normalizeRepeatedSlashes(input); got != expected {
			t.Fatalf("normalizeRepeatedSlashes(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestSanitizeRelativePath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty", input: "", want: ""},
		{name: "root_slash", input: "/", want: ""},
		{name: "nested", input: "a/b/file.txt", want: "a/b/file.txt"},
		{name: "leading_and_repeated_slashes", input: "/a//b///c", want: "a/b/c"},
		{name: "dot_segments", input: "a/./b/./c", want: "a/b/c"},
		{name: "traversal_parent", input: "../secret", wantErr: true},
		{name: "traversal_nested", input: "a/../../secret", wantErr: true},
		{name: "traversal_absolute_like", input: "/../secret", wantErr: true},
		{name: "invalid_char_space", input: "a b/file", wantErr: true},
		{name: "invalid_char_percent", input: "a/%2e%2e/file", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitizeRelativePath(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("sanitizeRelativePath(%q) expected error, got none", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("sanitizeRelativePath(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("sanitizeRelativePath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRouteFileRejectsTraversalPath(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "parent_traversal", url: "/static/../etc/passwd"},
		{name: "nested_parent_traversal", url: "/static/a/../../etc/passwd"},
		{name: "encoded_parent_segment", url: "/static/%2e%2e/etc/passwd"},
		{name: "invalid_segment_characters", url: "/static/a%20b/file.txt"},
	}

	server := &Server{ServeDir: root}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			req.Header.Set(HeaderSourceName, source)
			w := httptest.NewRecorder()

			server.routeFile(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("routeFile(%q) status=%d, want %d", tt.url, w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestRootRelativePath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty_maps_to_dot", input: "", want: "."},
		{name: "file_path_unchanged", input: "a/b.txt", want: "a/b.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rootRelativePath(tt.input)
			if got != tt.want {
				t.Fatalf("rootRelativePath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRouteFileRootListing(t *testing.T) {
	rootDir := t.TempDir()
	serveDir := filepath.Join(rootDir, "serve")
	sourceDir := filepath.Join(serveDir, source)
	if err := os.MkdirAll(filepath.Join(sourceDir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "top.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "nested", "child.txt"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}

	server := &Server{ServeDir: serveDir}
	req := httptest.NewRequest(http.MethodGet, "/static", nil)
	req.Header.Set(HeaderSourceName, source)
	w := httptest.NewRecorder()

	server.routeFile(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("routeFile(/static) status=%d, want %d", w.Code, http.StatusOK)
	}

	var names []string
	if err := json.Unmarshal(w.Body.Bytes(), &names); err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("routeFile(/static) names=%v, want 2 file entries", names)
	}
}

type mockGk struct {
	gotCleanedNow bool
	gotPruned     bool
	gotStopped    bool
	gotRecovered  bool
}

func (g *mockGk) Recover() {
	g.gotRecovered = true
}

func (g *mockGk) CleanNow() {
	g.gotCleanedNow = true
}

func (g *mockGk) Prune(t time.Duration) {
	g.gotPruned = true
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
