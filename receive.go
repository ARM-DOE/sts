package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
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
	StageDir string
	FinalDir string
	Port     int
	TLSCert  string
	TLSKey   string
	Sources  []string
	Keys     []string
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
	rcv := &Receiver{}
	rcv.Conf = conf
	rcv.fileLocks = make(map[string]*sync.Mutex)
	rcv.finalizer = f
	return rcv
}

// Serve starts HTTP server.
func (rcv *Receiver) Serve(stop <-chan bool) {
	http.Handle("/data", rcv.handleValidate(http.HandlerFunc(rcv.routeData)))
	http.Handle("/validate", rcv.handleValidate(http.HandlerFunc(rcv.routeValidate)))
	http.Handle("/partials", rcv.handleValidate(http.HandlerFunc(rcv.routePartials)))

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup, port int, cert, key string) {
		defer wg.Done()
		addr := fmt.Sprintf(":%d", port)
		var err error
		if cert != "" && key != "" {
			err = httputils.ListenAndServeTLS(addr, cert, key, nil)
		} else {
			err = httputils.ListenAndServe(addr, nil)
		}
		// According to:
		// https://golang.org/pkg/net/http/#Server.Serve
		// ...Serve() always returns a non-nil error.  I guess we'll ignore.
		if err != nil {
			// panic(err)
		}
	}(&wg, rcv.Conf.Port, rcv.Conf.TLSCert, rcv.Conf.TLSKey)
	<-stop
	httputils.Close()
	wg.Wait()
}

func (rcv *Receiver) handleValidate(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		source := r.Header.Get(HeaderSourceName)
		key := r.Header.Get(HeaderKey)
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
	source := r.Header.Get(HeaderSourceName)
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
			defer lock.Unlock()
			if _, err := os.Stat(base + CompExt); !os.IsNotExist(err) { // Make sure it still exists.
				cmp, err := ReadCompanion(base)
				if err == nil {
					// Would be nice not to have to care about case but apparently on Mac it can be an issue.
					relPath := strings.TrimPrefix(strings.ToLower(cmp.Path), strings.ToLower(root+string(os.PathSeparator)))
					cmp.Path = cmp.Path[len(root)+1 : len(root)+1+len(relPath)]
					partials = append(partials, cmp)
				}
			}
		}
		return nil
	})
	respJSON, err := json.Marshal(partials)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(respJSON)
}

func (rcv *Receiver) routeValidate(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(HeaderSourceName)
	files := []*ConfirmFile{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&files)
	if source == "" || err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	respMap := make(map[string]int, len(files))
	for _, f := range files {
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
	w.WriteHeader(http.StatusOK)
	w.Write(respJSON)
}

func (rcv *Receiver) routeData(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	source := r.Header.Get(HeaderSourceName)
	compressed := r.Header.Get("Content-Encoding") == "gzip"
	meta := r.Header.Get(HeaderBinData)
	reader, err := NewBinReader(meta, r.Body, compressed)
	if source == "" || err != nil {
		if err != nil {
			logging.Error(err.Error())
		}
		w.WriteHeader(http.StatusPartialContent) // respond with a 206
		return
	}
	n := 0
	for {
		next, eof := reader.Next()
		if eof { // Reached end of multipart request
			break
		}
		err = rcv.parsePart(next, source)
		if err != nil {
			logging.Error(err.Error())
			w.Header().Add(HeaderPartCount, strconv.Itoa(n))
			w.WriteHeader(http.StatusPartialContent) // respond with a 206
			return
		}
		n++
	}
	w.WriteHeader(http.StatusOK)
}

func (rcv *Receiver) getLock(path string) *sync.Mutex {
	rcv.lock.Lock()
	defer rcv.lock.Unlock()
	_, exists := rcv.fileLocks[path]
	if !exists {
		rcv.fileLocks[path] = &sync.Mutex{}
	}
	return rcv.fileLocks[path]
}

func (rcv *Receiver) initStageFile(filePath string, size int64) {
	lock := rcv.getLock(filePath)
	lock.Lock()
	defer lock.Unlock()
	var err error
	info, err := os.Stat(filePath + PartExt)
	if !os.IsNotExist(err) && info.Size() == size {
		return
	}
	if _, err = os.Stat(filePath + CompExt); !os.IsNotExist(err) {
		logging.Debug("RECEIVE Removing Stale Companion:", filePath+CompExt)
		os.Remove(filePath + CompExt)
	}
	os.MkdirAll(path.Dir(filePath), os.ModePerm)
	fh, err := os.Create(filePath + PartExt)
	logging.Debug(fmt.Sprintf("RECEIVE Creating Empty File: %s (%d B)", filePath, size))
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to create empty file at %s.%s with size %d: %s", filePath, PartExt, size, err.Error()))
	}
	fh.Truncate(size)
	fh.Close()
}

func (rcv *Receiver) parsePart(pr *PartReader, source string) (err error) {
	logging.Debug("RECEIVE Process Part:", pr.Meta.Path, pr.Meta.FileSize)

	path := filepath.Join(rcv.Conf.StageDir, source, pr.Meta.Path)
	hash := fileutils.NewMD5()

	rcv.initStageFile(path, pr.Meta.FileSize)

	// Read the part and write it to the right place in the staged "partial"
	fh, err := os.OpenFile(path+PartExt, os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("Failed to open file while trying to write part: %s", err.Error())
	}
	fh.Seek(pr.Meta.Beg, 0)
	logging.Debug("RECEIVE Seek:", pr.Meta.Path, pr.Meta.Beg)
	bytes := make([]byte, hash.BlockSize)
	var n int
	wrote := 0
	for {
		// The number of bytes read can often be less than the size of the passed buffer.
		n, err = pr.Read(bytes)
		// logging.Debug("RECEIVE Bytes Read", n, path, (pr.Meta.End-pr.Meta.Beg)-int64(wrote))
		wrote += n
		hash.Update(bytes[:n])
		fh.Write(bytes[:n])
		if err != nil {
			logging.Debug("RECEIVE:", err.Error())
			break
		}
	}
	logging.Debug("RECEIVE Wrote:", pr.Meta.Path, wrote)
	fh.Close()
	if pr.Meta.Hash != hash.Sum() {
		return fmt.Errorf("Bad part of %s from bytes %d:%d (%s:%s)", pr.Meta.Path, pr.Meta.Beg, pr.Meta.End, pr.Meta.Hash, hash.Sum())
	}

	// Make sure we're the only one updating the companion
	lock := rcv.getLock(path)
	lock.Lock()
	defer lock.Unlock()

	// Update the companion file
	cmp, err := NewCompanion(source, path, pr.Meta.PrevPath, pr.Meta.FileSize, pr.Meta.FileHash)
	if err != nil {
		return err
	}
	cmp.AddPart(pr.Meta.Hash, pr.Meta.Beg, pr.Meta.End)
	err = cmp.Write()
	if err != nil {
		return fmt.Errorf("Failed to write updated companion: %s", err.Error())
	}
	if cmp.IsComplete() {

		// Finish file by removing the "partial" extension so the scanner will pick it up.
		err := os.Rename(path+PartExt, path)
		if err != nil {
			return err
		}
		logging.Debug("RECEIVE File Done:", path)
	}
	logging.Debug("RECEIVE Part OK:", path)
	return nil
}
