package send

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/bin"
	"code.arm.gov/dataflow/sts/httputil"
)

type HTTP struct {
	client *http.Client
	conf   *sts.SendConf
}

func NewHTTP(conf *sts.SendConf) (http *HTTP, err error) {
	http = &HTTP{
		conf: conf,
	}
	if http.client, err = httputil.GetClient(conf.TLS); err != nil {
		return
	}
	http.client.Timeout = time.Second * conf.Timeout
	return
}

func (h *HTTP) SendBin(b *bin.Bin) (n int, err error) {
	var gz *gzip.Writer
	if h.conf.Compression != gzip.NoCompression {
		gz, err = gzip.NewWriterLevel(nil, h.conf.Compression)
		if err != nil {
			return
		}
	}
	br := bin.NewEncoder(b)
	jr, err := httputil.GetJSONReader(br.Meta, gzip.NoCompression)
	if err != nil {
		return
	}
	// We have to read it all into memory in order to calculate the length so the
	// receiving end knows how much of the beginning of the payload belongs to
	// metadata.
	var meta []byte
	if meta, err = ioutil.ReadAll(jr); err != nil {
		return
	}
	mr := bytes.NewReader(meta)
	pr, pw := io.Pipe()
	go func() {
		if gz != nil {
			gz.Reset(pw)
			io.Copy(gz, mr)
			io.Copy(gz, br)
			gz.Close()
		} else {
			io.Copy(pw, mr)
			io.Copy(pw, br)
		}
		pw.Close()
	}()
	url := fmt.Sprintf("%s://%s/data", h.conf.Protocol(), h.conf.TargetHost)
	req, err := http.NewRequest("PUT", url, pr)
	if err != nil {
		return
	}
	req.Header.Add(httputil.HeaderSourceName, h.conf.SourceName)
	req.Header.Add(httputil.HeaderMetaLen, strconv.Itoa(len(meta)))
	req.Header.Add(httputil.HeaderSep, string(os.PathSeparator))
	if h.conf.TargetKey != "" {
		req.Header.Add(httputil.HeaderKey, h.conf.TargetKey)
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
		n, _ = strconv.Atoi(resp.Header.Get(httputil.HeaderPartCount))
		err = fmt.Errorf("Bin failed validation. Successful part(s): %s", resp.Header.Get(httputil.HeaderPartCount))
		return
	} else if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bin failed with response code: %d", resp.StatusCode)
		return
	}
	n = len(b.Parts)
	return
}
