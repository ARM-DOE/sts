package http

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
)

type confirmable struct {
	Name    string `json:"n"`
	Started int64  `json:"t"` // Expects Unix
}

func (c *confirmable) GetName() string {
	return c.Name
}

func (c *confirmable) GetStarted() time.Time {
	return time.Unix(c.Started, 0)
}

type confirmed struct {
	name string
	code int
}

func (c *confirmed) GetName() string {
	return c.name
}

func (c *confirmed) NotFound() bool {
	return c.code == sts.ConfirmNone
}

func (c *confirmed) Waiting() bool {
	return c.code == sts.ConfirmWaiting
}

func (c *confirmed) Failed() bool {
	return c.code == sts.ConfirmFailed
}

func (c *confirmed) Received() bool {
	return c.code == sts.ConfirmPassed
}

// Client is the main client struct
type Client struct {
	SourceName  string
	TargetHost  string
	TargetPort  int
	TargetKey   string
	Compression int
	Timeout     time.Duration
	TLS         *tls.Config

	root   string
	client *http.Client
}

func (h *Client) rootURL() string {
	if h.root != "" {
		return h.root
	}
	protocol := "http"
	if h.TLS != nil {
		protocol += "s"
	}
	h.root = fmt.Sprintf("%s://%s:%d", protocol, h.TargetHost, h.TargetPort)
	return h.root
}

func (h *Client) init() error {
	if h.client != nil {
		return nil
	}
	var err error
	if h.client, err = GetClient(h.TLS); err != nil {
		return err
	}
	h.client.Timeout = time.Second * h.Timeout
	return nil
}

// Transmit is of type sts.Transmit for sending a payload
func (h *Client) Transmit(payload sts.Payload) (n int, err error) {
	if err = h.init(); err != nil {
		return
	}
	var gz *gzip.Writer
	if h.Compression != gzip.NoCompression {
		gz, err = gzip.NewWriterLevel(nil, h.Compression)
		if err != nil {
			return
		}
	}
	var meta []byte
	if meta, err = payload.EncodeHeader(); err != nil {
		return
	}
	mr := bytes.NewReader(meta)
	dr := payload.GetEncoder()
	pr, pw := io.Pipe()
	go func() {
		if gz != nil {
			gz.Reset(pw)
			io.Copy(gz, mr)
			io.Copy(gz, dr)
			gz.Close()
		} else {
			io.Copy(pw, mr)
			io.Copy(pw, dr)
		}
		pw.Close()
	}()
	url := fmt.Sprintf("%s/data", h.rootURL())
	req, err := http.NewRequest("PUT", url, pr)
	if err != nil {
		return
	}
	req.Header.Add(HeaderSourceName, h.SourceName)
	req.Header.Add(HeaderMetaLen, strconv.Itoa(len(meta)))
	req.Header.Add(HeaderSep, string(os.PathSeparator))
	if h.TargetKey != "" {
		req.Header.Add(HeaderKey, h.TargetKey)
	}
	if gz != nil {
		req.Header.Add("Content-Encoding", "gzip")
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPartialContent {
		n, _ = strconv.Atoi(resp.Header.Get(HeaderPartCount))
		err = fmt.Errorf(
			"Bin failed validation. Successful part(s): %s",
			resp.Header.Get(HeaderPartCount))
		return
	} else if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bin failed with response code: %d", resp.StatusCode)
		return
	}
	n = len(payload.GetParts())
	return
}

// Validate is of type sts.Validate for confirming or denying validity of
// transmitted files
func (h *Client) Validate(sent []sts.Pollable) (polled []sts.Polled, err error) {
	if err = h.init(); err != nil {
		return
	}
	fmap := make(map[string]sts.Pollable)
	var cf []*confirmable
	for _, f := range sent {
		cf = append(cf, &confirmable{
			Name:    f.GetName(),
			Started: f.GetStarted().Unix(),
		})
		fmap[f.GetName()] = f
		log.Debug("POLLing:", f.GetName())
	}
	url := fmt.Sprintf("%s/validate", h.rootURL())
	r, err := GetJSONReader(cf, h.Compression)
	if err != nil {
		return
	}
	req, err := http.NewRequest("POST", url, r)
	if h.Compression != gzip.NoCompression {
		req.Header.Add(HeaderContentEncoding, HeaderGzip)
	}
	req.Header.Add(HeaderContentType, HeaderJSON)
	req.Header.Add(HeaderSourceName, h.SourceName)
	req.Header.Add(HeaderSep, string(os.PathSeparator))
	if h.TargetKey != "" {
		req.Header.Add(HeaderKey, h.TargetKey)
	}
	if err != nil {
		return
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("Poll request failed: %d", resp.StatusCode)
		return
	}
	sep := resp.Header.Get(HeaderSep)
	reader, err := GetRespReader(resp)
	if err != nil {
		return
	}
	defer reader.Close()
	fileMap := make(map[string]int)
	jsonDecoder := json.NewDecoder(reader)
	err = jsonDecoder.Decode(&fileMap)
	if err != nil {
		return
	}
	for path, code := range fileMap {
		if sep != "" {
			path = filepath.Join(strings.Split(path, sep)...)
		}
		log.Debug("POLL Result:", path, code)
		orig := fmap[path]
		polled = append(polled, &confirmed{
			name: orig.GetName(),
			code: code,
		})
	}
	return
}

// Recover is of type sts.Recoverer and is responsible for retrieving partial
// files on the target
func (h *Client) Recover() (partials []*sts.Partial, err error) {
	if err = h.init(); err != nil {
		return
	}
	var req *http.Request
	var resp *http.Response
	var reader io.ReadCloser
	url := fmt.Sprintf("%s/partials", h.rootURL())
	if req, err = http.NewRequest("GET", url, bytes.NewReader([]byte(""))); err != nil {
		return
	}
	req.Header.Add(HeaderSourceName, h.SourceName)
	if h.TargetKey != "" {
		req.Header.Add(HeaderKey, h.TargetKey)
	}
	if resp, err = h.client.Do(req); err != nil {
		return
	}
	if reader, err = GetRespReader(resp); err != nil {
		return
	}
	defer reader.Close()
	partials = []*sts.Partial{}
	jsonDecoder := json.NewDecoder(reader)
	err = jsonDecoder.Decode(&partials)
	if err != nil {
		return
	}
	return
}
