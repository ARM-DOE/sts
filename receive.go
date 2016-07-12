package main

import (
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
)

// PartExt is the file extension added to files in the stage area as they are
// received.
const PartExt = ".part"

// ReceiverConf struct contains configuration parameters needed to run.
type ReceiverConf struct {
	StageDir    string
	FinalDir    string
	Port        int
	TLS         *tls.Config
	Sources     []string
	Keys        []string
	Compression int
}

func (c *ReceiverConf) isValid(src, key string) bool {
	if src != "" {
		if matched, err := regexp.MatchString(`^[a-z0-9\.\-]+$`, src); err != nil || !matched {
			return false
		}
		if len(c.Sources) > 0 && strToIndex(src, c.Sources) < 0 {
			return false
		}
	}
	if key != "" && len(c.Keys) > 0 && strToIndex(key, c.Keys) < 0 {
		return false
	}
	return true
}

func strToIndex(needle string, haystack []string) int {
	for i, v := range haystack {
		if v == needle {
			return i
		}
	}
	return -1
}

// Receiver struct contains the configuration and finalizer references.
type Receiver struct {
	Conf      *ReceiverConf
	lock      sync.Mutex
	fileLocks map[string]*sync.Mutex
	finalizer *Finalizer
}

// NewReceiver creates new receiver instance according to the input configuration.
// A reference to a finalizer instance is necessary in order for the validation route
// to be able to confirm files received.
func NewReceiver(conf *ReceiverConf, f *Finalizer) *Receiver {
	r := &Receiver{}
	r.Conf = conf
	r.fileLocks = make(map[string]*sync.Mutex)
	r.finalizer = f
	return r
}

// Serve starts HTTP server.
func (rcv *Receiver) Serve(stop <-chan bool) {
	http.Handle("/data", rcv.handleValidate(http.HandlerFunc(rcv.routeData)))
	http.Handle("/validate", rcv.handleValidate(http.HandlerFunc(rcv.routeValidate)))
	http.Handle("/partials", rcv.handleValidate(http.HandlerFunc(rcv.routePartials)))

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup, port int, tlsConf *tls.Config) {
		defer wg.Done()
		addr := fmt.Sprintf(":%d", port)
		err := httputils.ListenAndServe(addr, tlsConf, nil)
		// According to:
		// https://golang.org/pkg/net/http/#Server.Serve
		// ...Serve() always returns a non-nil error.  I guess we'll ignore.
		if err != nil {
			// panic(err)
		}
	}(&wg, rcv.Conf.Port, rcv.Conf.TLS)
	<-stop
	httputils.Close()
	wg.Wait()
}

func (rcv *Receiver) handleValidate(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		source := r.Header.Get(httputils.HeaderSourceName)
		key := r.Header.Get(httputils.HeaderKey)
		if !rcv.Conf.isValid(source, key) {
			logging.Error(fmt.Errorf("Unknown Source:Key => %s:%s", source, key))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func (rcv *Receiver) routePartials(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(httputils.HeaderSourceName)
	if source == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var partials []*Companion
	root := filepath.Join(rcv.Conf.StageDir, source)
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == CompExt {
			base := strings.TrimSuffix(path, CompExt)
			lock := rcv.getLock(base)
			lock.Lock()
			if _, err := os.Stat(base + CompExt); !os.IsNotExist(err) { // Make sure it still exists.
				cmp, err := ReadCompanion(base)
				if err == nil {
					// Would be nice not to have to care about case but apparently on Mac it can be an issue.
					relPath := strings.TrimPrefix(strings.ToLower(cmp.Path), strings.ToLower(root+string(os.PathSeparator)))
					cmp.Path = cmp.Path[len(root)+1 : len(root)+1+len(relPath)]
					partials = append(partials, cmp)
				}
			}
			lock.Unlock()
			rcv.delLock(base)
		}
		return nil
	})
	respJSON, err := json.Marshal(partials)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.Header().Set(httputils.HeaderSep, string(os.PathSeparator))
	w.Header().Set(httputils.HeaderContentType, httputils.HeaderJSON)
	if err := rcv.respond(w, http.StatusOK, respJSON); err != nil {
		logging.Error(err.Error())
	}
}

func (rcv *Receiver) routeValidate(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(httputils.HeaderSourceName)
	sep := r.Header.Get(httputils.HeaderSep)
	files := []*ConfirmFile{}
	var br io.Reader
	var err error
	if br, err = httputils.GetReqReader(r); err != nil {
		logging.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	decoder := json.NewDecoder(br)
	err = decoder.Decode(&files)
	if source == "" || err != nil {
		if err != nil {
			logging.Error(err.Error())
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	respMap := make(map[string]int, len(files))
	for _, f := range files {
		if sep != "" {
			f.RelPath = filepath.Join(strings.Split(f.RelPath, sep)...)
		}
		logging.Debug("SEARCH", source, f.RelPath)
		success, found := rcv.finalizer.IsFinal(source, f.RelPath, time.Unix(f.Started, 0))
		code := ConfirmNone
		if found && success {
			code = ConfirmPassed
		} else if found && !success {
			code = ConfirmFailed
		}
		respMap[f.RelPath] = code
	}
	respJSON, _ := json.Marshal(respMap)
	w.Header().Set(httputils.HeaderSep, string(os.PathSeparator))
	w.Header().Set(httputils.HeaderContentType, httputils.HeaderJSON)
	if err := rcv.respond(w, http.StatusOK, respJSON); err != nil {
		logging.Error(err.Error())
	}
}

func (rcv *Receiver) routeData(w http.ResponseWriter, r *http.Request) {
	var err error
	defer r.Body.Close()
	source := r.Header.Get(httputils.HeaderSourceName)
	compressed := r.Header.Get(httputils.HeaderContentEncoding) == httputils.HeaderGzip
	sep := r.Header.Get(httputils.HeaderSep)
	var mlen int
	if mlen, err = strconv.Atoi(r.Header.Get(httputils.HeaderMetaLen)); err != nil {
		logging.Error(err.Error())
		return
	}
	rr := r.Body
	if compressed {
		if rr, err = gzip.NewReader(rr); err != nil {
			logging.Error(err.Error())
			return
		}
	}
	br, err := NewBinDecoder(rr, mlen, sep)
	if source == "" || err != nil {
		if err != nil {
			logging.Error(err.Error())
		}
		w.WriteHeader(http.StatusPartialContent) // respond with a 206
		return
	}
	var parts []*PartDecoder
	for {
		next, eof := br.Next()
		if eof { // Reached end of multipart request
			w.WriteHeader(http.StatusOK)
			break
		}
		err := rcv.parsePart(next, source)
		if err != nil {
			logging.Error(err.Error())
			w.Header().Add(httputils.HeaderPartCount, strconv.Itoa(len(parts)))
			w.WriteHeader(http.StatusPartialContent) // respond with a 206
			break
		}
		parts = append(parts, next)
	}
	if len(parts) == 0 {
		return
	}
	// Better to handle companion updates after sending response in case we crash
	// before being able to let the sender know.
	go func() {
		for _, pr := range parts {
			rcv.completePart(pr, source)
		}
	}()
}

func (rcv *Receiver) respond(w http.ResponseWriter, status int, data []byte) error {
	if rcv.Conf.Compression != gzip.NoCompression {
		gz, err := gzip.NewWriterLevel(w, rcv.Conf.Compression)
		if err != nil {
			w.Write(data)
			return err
		}
		w.Header().Add("Content-Encoding", "gzip")
		w.WriteHeader(status)
		defer gz.Close()
		gz.Write(data)
		return nil
	}
	w.Write(data)
	return nil
}

func (rcv *Receiver) getLock(path string) *sync.Mutex {
	rcv.lock.Lock()
	defer rcv.lock.Unlock()
	var m *sync.Mutex
	var exists bool
	if m, exists = rcv.fileLocks[path]; !exists {
		m = &sync.Mutex{}
		rcv.fileLocks[path] = m
	}
	return m
}

func (rcv *Receiver) delLock(path string) {
	rcv.lock.Lock()
	defer rcv.lock.Unlock()
	delete(rcv.fileLocks, path)
}

func (rcv *Receiver) initStage(path string, size int64) {
	lock := rcv.getLock(path)
	lock.Lock()
	defer lock.Unlock()
	var err error
	info, err := os.Stat(path + PartExt)
	if !os.IsNotExist(err) && info.Size() == size {
		return
	}
	if _, err = os.Stat(path + CompExt); !os.IsNotExist(err) {
		logging.Debug("RECEIVE Removing Stale Companion:", path+CompExt)
		os.Remove(path + CompExt)
	}
	logging.Debug("RECEIVE Making Directory:", path, filepath.Dir(path))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		logging.Error(err.Error())
		return
	}
	fh, err := os.Create(path + PartExt)
	logging.Debug(fmt.Sprintf("RECEIVE Creating Empty File: %s (%d B)", path, size))
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to create empty file at %s%s with size %d: %s", path, PartExt, size, err.Error()))
	}
	defer fh.Close()
	fh.Truncate(size)
}

func (rcv *Receiver) parsePart(pr *PartDecoder, source string) (err error) {
	path := filepath.Join(rcv.Conf.StageDir, source, pr.Meta.Path)

	rcv.initStage(path, pr.Meta.FileSize)

	// Read the part and write it to the right place in the staged "partial"
	fh, err := os.OpenFile(path+PartExt, os.O_WRONLY, 0600)
	if err != nil {
		err = fmt.Errorf("Failed to open file while trying to write part: %s", err.Error())
		return
	}
	fh.Seek(pr.Meta.Beg, 0)
	n, err := io.Copy(fh, pr)
	fh.Close()
	logging.Debug("RECEIVE Wrote:", pr.Meta.Path, pr.Meta.Beg, n)
	if err != nil {
		return
	}
	if pr.Meta.Hash != "" && pr.Meta.Hash != fileutils.HashHex(pr.Hash) {
		err = fmt.Errorf("Bad part of %s from bytes %d:%d (%s:%s)", pr.Meta.Path, pr.Meta.Beg, pr.Meta.End, pr.Meta.Hash, fileutils.HashHex(pr.Hash))
		return
	}
	logging.Debug("RECEIVE Part OK:", path, n)
	return
}

func (rcv *Receiver) completePart(pr *PartDecoder, source string) {
	path := filepath.Join(rcv.Conf.StageDir, source, pr.Meta.Path)

	// Make sure we're the only one updating the companion
	lock := rcv.getLock(path)
	lock.Lock()
	defer lock.Unlock()

	// Update the companion file
	cmp, err := NewCompanion(source, path, pr.Meta.PrevPath, pr.Meta.FileSize, pr.Meta.FileHash)
	if err != nil {
		return
	}
	cmp.AddPart(pr.Meta.Hash, pr.Meta.Beg, pr.Meta.End)
	err = cmp.Write()
	if err != nil {
		err = fmt.Errorf("Failed to write updated companion: %s", err.Error())
		return
	}
	if cmp.IsComplete() {
		defer rcv.delLock(path)

		// Touch the file with current time to make sure it can't get picked up immediately.
		err = os.Chtimes(path+PartExt, time.Now(), time.Now())
		if err != nil {
			logging.Error(err.Error())
		}
		// Finish file by removing the "partial" extension so the scanner will pick it up.
		err = os.Rename(path+PartExt, path)
		if err != nil {
			logging.Error(err.Error())
			return
		}
		logging.Debug("RECEIVE File Done:", path)
	}
}
