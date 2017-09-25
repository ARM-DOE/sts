package server

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

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/bin"
	"code.arm.gov/dataflow/sts/companion"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/httputil"
	"code.arm.gov/dataflow/sts/logging"
)

// PartExt is the file extension added to files in the stage area as they are
// received.
const PartExt = ".part"

type recvFile struct {
	path    string
	relPath string
	size    int64
	time    int64
	comp    *companion.Companion
}

func (f *recvFile) GetPath(follow bool) string {
	// Symbolic links are irrelevant for received files
	return f.path
}
func (f *recvFile) GetRelPath() string {
	return f.relPath
}
func (f *recvFile) GetSize() int64 {
	f.Reset()
	return f.size
}
func (f *recvFile) GetTime() int64 {
	f.Reset()
	return f.time
}
func (f *recvFile) Reset() (changed bool, err error) {
	var info os.FileInfo
	if info, err = os.Stat(f.path); err != nil {
		return
	}
	s := info.Size()
	t := info.ModTime().Unix()
	if s != f.size {
		f.size = s
		changed = true
	}
	if t != f.time {
		f.time = t
		changed = true
	}
	return
}
func (f *recvFile) GetCompanion() *companion.Companion {
	return f.comp
}

// ReceiverConf struct contains configuration parameters needed to run.
type ReceiverConf struct {
	ServeDir    string
	StageDir    string
	FinalDir    string
	Host        string
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
	finalizer sts.FinalStatusService
	outChan   chan<- []sts.File
}

// NewReceiver creates new receiver instance according to the input configuration.
// A reference to a finalizer instance is necessary in order for the validation route
// to be able to confirm files received.
func NewReceiver(conf *ReceiverConf, f sts.FinalStatusService) *Receiver {
	r := &Receiver{}
	r.Conf = conf
	r.fileLocks = make(map[string]*sync.Mutex)
	r.finalizer = f
	return r
}

// Serve starts HTTP server.
func (rcv *Receiver) Serve(out chan<- []sts.File, stop <-chan bool) {
	rcv.outChan = out

	http.Handle("/data", rcv.handleValidate(http.HandlerFunc(rcv.routeData)))
	http.Handle("/validate", rcv.handleValidate(http.HandlerFunc(rcv.routeValidate)))
	http.Handle("/partials", rcv.handleValidate(http.HandlerFunc(rcv.routePartials)))
	http.Handle("/static/", rcv.handleValidate(http.HandlerFunc(rcv.routeFile)))

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup, port int, tlsConf *tls.Config) {
		defer wg.Done()
		addr := fmt.Sprintf("%s:%d", rcv.Conf.Host, port)
		err := httputil.ListenAndServe(addr, tlsConf, nil)
		if err != http.ErrServerClosed {
			logging.Error(err.Error())
		}
	}(&wg, rcv.Conf.Port, rcv.Conf.TLS)
	<-stop
	httputil.Close()
	wg.Wait()
}

func (rcv *Receiver) handleValidate(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		source := r.Header.Get(httputil.HeaderSourceName)
		key := r.Header.Get(httputil.HeaderKey)
		if !rcv.Conf.isValid(source, key) {
			logging.Error(fmt.Errorf("Unknown Source:Key => %s:%s", source, key))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func (rcv *Receiver) routeFile(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(httputil.HeaderSourceName)
	if source == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file := r.URL.Path[len("/static/"):]
	root := filepath.Join(rcv.Conf.ServeDir, source)
	path := filepath.Join(root, file)
	fi, err := os.Stat(path)
	switch r.Method {
	case http.MethodGet:
		var data interface{}
		switch {
		case os.IsNotExist(err):
			w.WriteHeader(http.StatusNotFound)
			return
		case fi.IsDir():
			var names []string
			handleNode := func(path string, info os.FileInfo, err error) error {
				if info == nil || err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				names = append(names, path[len(root)+1:])
				return nil
			}
			if err = fileutil.Walk(path, handleNode, true); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			data = names
			var respJSON []byte
			respJSON, err = json.Marshal(data)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set(httputil.HeaderContentType, httputil.HeaderJSON)
			if err = rcv.respond(w, http.StatusOK, respJSON); err != nil {
				logging.Error(err.Error())
			}
		default:
			var fp *os.File
			fp, err = os.Open(path)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer fp.Close()
			io.Copy(w, fp)
		}
	case http.MethodDelete:
		switch {
		case os.IsNotExist(err):
			w.WriteHeader(http.StatusNotFound)
			return
		case fi.IsDir():
			w.WriteHeader(http.StatusBadRequest)
			return
		default:
			err = os.Remove(path)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (rcv *Receiver) routePartials(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(httputil.HeaderSourceName)
	if source == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var partials []*companion.Companion
	root := filepath.Join(rcv.Conf.StageDir, source)
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == companion.CompExt {
			base := strings.TrimSuffix(path, companion.CompExt)
			lock := rcv.getLock(base)
			lock.Lock()
			if _, err := os.Stat(base + companion.CompExt); !os.IsNotExist(err) { // Make sure it still exists.
				cmp, err := companion.ReadCompanion(base)
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
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set(httputil.HeaderSep, string(os.PathSeparator))
	w.Header().Set(httputil.HeaderContentType, httputil.HeaderJSON)
	if err := rcv.respond(w, http.StatusOK, respJSON); err != nil {
		logging.Error(err.Error())
	}
}

func (rcv *Receiver) routeValidate(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(httputil.HeaderSourceName)
	sep := r.Header.Get(httputil.HeaderSep)
	files := []*sts.ConfirmFile{}
	var br io.Reader
	var err error
	if br, err = httputil.GetReqReader(r); err != nil {
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
		respMap[f.RelPath] = rcv.finalizer.GetFileStatus(source, f.RelPath, time.Unix(f.Started, 0))
	}
	respJSON, _ := json.Marshal(respMap)
	w.Header().Set(httputil.HeaderSep, string(os.PathSeparator))
	w.Header().Set(httputil.HeaderContentType, httputil.HeaderJSON)
	if err := rcv.respond(w, http.StatusOK, respJSON); err != nil {
		logging.Error(err.Error())
	}
}

func (rcv *Receiver) routeData(w http.ResponseWriter, r *http.Request) {
	var err error
	defer r.Body.Close()
	source := r.Header.Get(httputil.HeaderSourceName)
	compressed := r.Header.Get(httputil.HeaderContentEncoding) == httputil.HeaderGzip
	sep := r.Header.Get(httputil.HeaderSep)
	var mlen int
	if mlen, err = strconv.Atoi(r.Header.Get(httputil.HeaderMetaLen)); err != nil {
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
	br, err := bin.NewDecoder(rr, mlen, sep)
	if source == "" || err != nil {
		if err != nil {
			logging.Error(err.Error())
		}
		w.WriteHeader(http.StatusPartialContent) // respond with a 206
		return
	}
	var parts []*bin.PartDecoder
	for {
		next, eof := br.Next()
		if eof { // Reached end of multipart request
			w.WriteHeader(http.StatusOK)
			break
		}
		err := rcv.writePart(next, source)
		if err != nil {
			logging.Error(err.Error())
			w.Header().Add(httputil.HeaderPartCount, strconv.Itoa(len(parts)))
			w.WriteHeader(http.StatusPartialContent) // respond with a 206
			break
		}
		parts = append(parts, next)
	}
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
	if _, err = os.Stat(path + companion.CompExt); !os.IsNotExist(err) {
		logging.Debug("RECEIVE Removing Stale Companion:", path+companion.CompExt)
		os.Remove(path + companion.CompExt)
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

func (rcv *Receiver) writePart(pr *bin.PartDecoder, source string) (err error) {
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
	if pr.Meta.Hash != "" && pr.Meta.Hash != fileutil.HashHex(pr.Hash) {
		err = fmt.Errorf("Bad part of %s from bytes %d:%d (%s:%s)", pr.Meta.Path, pr.Meta.Beg, pr.Meta.End, pr.Meta.Hash, fileutil.HashHex(pr.Hash))
		return
	}
	logging.Debug("RECEIVE Part OK:", path, n)

	// Make sure we're the only one updating the companion
	lock := rcv.getLock(path)
	lock.Lock()
	defer lock.Unlock()

	// Update the companion file
	cmp, err := companion.NewCompanion(source, path, pr.Meta.PrevPath, pr.Meta.FileSize, pr.Meta.FileHash)
	if err != nil {
		return err
	}
	cmp.AddPart(pr.Meta.Hash, pr.Meta.Beg, pr.Meta.End)
	if err = cmp.Write(); err != nil {
		return fmt.Errorf("Failed to write updated companion: %s", err.Error())
	}
	if cmp.IsComplete() {
		defer rcv.delLock(path)

		// Finish file by removing the "partial" extension so the scanner will pick it up
		// on startup in case something does wrong between now and when it's finalized.
		if err = os.Rename(path+PartExt, path); err != nil {
			return fmt.Errorf("Failed to drop \"partial\" extension: %s", err.Error())
		}
		logging.Debug("RECEIVE File Done:", path)

		// Use a go routine in case the finalize channel is full.  We don't want to hold
		// up the response.
		go func() {
			// Add to the finalize channel
			rcv.outChan <- []sts.File{&recvFile{path: path, relPath: pr.Meta.Path, comp: cmp}}
		}()
	}
	return nil
}
