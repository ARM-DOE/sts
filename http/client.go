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

	"github.com/arm-doe/sts"
)

const apiVersion = 1

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
	sts.Pollable
	code int
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
	SourceName           string
	TargetHost           string
	TargetPort           int
	TargetPrefix         string
	TargetKey            string
	Compression          int
	Timeout              time.Duration
	TLS                  *tls.Config
	PartialsDecoder      sts.DecodePartials
	BandwidthLogInterval time.Duration

	root   string
	client *BandwidthLoggingClient
}

func (h *Client) rootURL() string {
	if h.root != "" {
		return h.root
	}
	protocol := "http"
	if h.TLS != nil || h.TargetPort == 443 {
		protocol += "s"
	}
	h.root = fmt.Sprintf("%s://%s:%d", protocol, h.TargetHost, h.TargetPort)
	if h.TargetPrefix != "" {
		h.root += h.TargetPrefix
	}
	return h.root
}

func (h *Client) init() error {
	if h.client != nil {
		return nil
	}
	var err error
	if h.client, err = GetClient(
		h.TLS,
		h.BandwidthLogInterval,
		fmt.Sprintf("(%s) ", h.SourceName)); err != nil {
		return err
	}
	h.client.Timeout = h.Timeout
	return nil
}

func (h *Client) Destroy() {
	if h.client != nil {
		h.client.Stop()
	}
}

// Transmit is of type sts.Transmit for sending a payload
func (h *Client) Transmit(payload sts.Payload) (n int, err error) {
	if err = h.init(); err != nil {
		return
	}
	var gz *gzip.Writer
	if h.Compression != gzip.NoCompression {
		if gz, err = gzip.NewWriterLevel(nil, h.Compression); err != nil {
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
	defer dr.Close()
	go func() {
		if gz != nil {
			gz.Reset(pw)
			_, _ = io.Copy(gz, mr)
			_, _ = io.Copy(gz, dr)
			_ = gz.Close()
		} else {
			_, _ = io.Copy(pw, mr)
			_, _ = io.Copy(pw, dr)
		}
		pw.Close()
	}()
	url := fmt.Sprintf("%s/data?v=%d", h.rootURL(), apiVersion)
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
			"bin failed validation; successful part(s): %s",
			resp.Header.Get(HeaderPartCount))
		return
	} else if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("bin failed with response code: %d", resp.StatusCode)
		return
	}
	n = len(payload.GetParts())
	return
}

// RecoverTransmission is of type sts.RecoverTransmission for querying how
// many parts of a failed transmission were successfully received.
func (h *Client) RecoverTransmission(payload sts.Payload) (n int, err error) {
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
	pr, pw := io.Pipe()
	go func() {
		if gz != nil {
			gz.Reset(pw)
			_, _ = io.Copy(gz, mr)
			gz.Close()
		} else {
			_, _ = io.Copy(pw, mr)
		}
		pw.Close()
	}()
	url := fmt.Sprintf("%s/data-recovery?v=%d", h.rootURL(), apiVersion)
	req, err := http.NewRequest("PUT", url, pr)
	if err != nil {
		return
	}
	req.Header.Add(HeaderSourceName, h.SourceName)
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
	switch resp.StatusCode {
	case http.StatusOK:
		n, _ = strconv.Atoi(resp.Header.Get(HeaderPartCount))
		return
	default:
		err = fmt.Errorf(
			"transmission recovery request failed with response code: %d",
			resp.StatusCode)
	}
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
	}
	url := fmt.Sprintf("%s/validate?v=%d", h.rootURL(), apiVersion)
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
		err = fmt.Errorf("poll request failed: %d", resp.StatusCode)
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
		orig := fmap[path]
		polled = append(polled, &confirmed{
			Pollable: orig,
			code:     code,
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
	url := fmt.Sprintf("%s/partials?v=%d", h.rootURL(), apiVersion)
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
	partials, err = h.PartialsDecoder(reader)
	return
}
