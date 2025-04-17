package http

import (
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/fileutil"
	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/marshal"
	"go.bryk.io/pkg/net/middleware/hsts"
)

// Server responds to client HTTP requests
type Server struct {
	ServeDir    string
	Host        string
	Port        int
	PathPrefix  string
	TLS         *tls.Config
	Compression int
	HSTS        *hsts.Options

	GateKeepers       map[string]sts.GateKeeper
	GateKeeperFactory sts.GateKeeperFactory
	DecoderFactory    sts.PayloadDecoderFactory

	IsValid       sts.RequestValidator
	ClientManager sts.ClientManager

	ChanceOfSimulatedFailure float64

	lock sync.RWMutex
}

func (s *Server) getGateKeeper(r *http.Request) sts.GateKeeper {
	source := getSourceName(r)
	if source == "" {
		return nil
	}
	s.lock.RLock()
	if gk, ok := s.GateKeepers[source]; ok {
		s.lock.RUnlock()
		return gk
	}
	s.lock.RUnlock()
	gk := s.GateKeeperFactory(source)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.GateKeepers[source] = gk
	return gk
}

func (s *Server) stopGateKeepers() {
	s.lock.Lock()
	defer s.lock.Unlock()
	wg := sync.WaitGroup{}
	wg.Add(len(s.GateKeepers))
	for _, gk := range s.GateKeepers {
		go func(gk sts.GateKeeper) {
			defer wg.Done()
			gk.Stop(false)
		}(gk)
	}
	wg.Wait()
}

// Serve starts HTTP server.
func (s *Server) Serve(stop <-chan bool, done chan<- bool) {
	http.Handle(s.PathPrefix+"/", s.handle(http.HandlerFunc(s.routeHealthCheck)))
	http.Handle(s.PathPrefix+"/client/", s.handle(http.HandlerFunc(s.routeClientManagement)))
	http.Handle(s.PathPrefix+"/data", s.handle(s.handleValidate(http.HandlerFunc(s.routeData))))
	http.Handle(s.PathPrefix+"/data-recovery", s.handle(s.handleValidate(http.HandlerFunc(s.routeDataRecovery))))
	http.Handle(s.PathPrefix+"/validate", s.handle(s.handleValidate(http.HandlerFunc(s.routeValidate))))
	http.Handle(s.PathPrefix+"/partials", s.handle(s.handleValidate(http.HandlerFunc(s.routePartials))))
	http.Handle(s.PathPrefix+"/static/", s.handle(s.handleValidate(http.HandlerFunc(s.routeFile))))
	http.Handle(s.PathPrefix+"/check-mapping", s.handle(http.HandlerFunc(s.routeCheckMapping)))

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		addr := fmt.Sprintf("%s:%d", s.Host, s.Port)
		err := ListenAndServe(addr, s.TLS, nil)
		if err != http.ErrServerClosed {
			log.Error("Error shutting down server:", err.Error())
		}
		s.stopGateKeepers()
	}()

	iServer := NewGracefulServer(&http.Server{
		Addr:    fmt.Sprintf(":%d", s.Port+1),
		Handler: http.HandlerFunc(s.routeInternal),
	}, nil)

	go func() {
		defer wg.Done()
		err := iServer.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Error("Error shutting down internal server:", err.Error())
		}
	}()

	<-stop

	_ = Close()
	iServer.Close()

	wg.Wait()

	done <- true
	DefaultServer = nil
}

func (s *Server) handle(next http.Handler) http.Handler {
	if s.HSTS != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if s.TLS != nil {
				// SJB: can't figure out why these aren't being set automatically but they
				// are important for the HSTS middleware to work correctly
				if r.URL.Scheme == "" {
					r.URL.Scheme = "https"
				}
				if r.TLS == nil {
					r.TLS = &tls.ConnectionState{
						HandshakeComplete: true,
					}
				}
			}
			hsts.Handler(*s.HSTS)(next).ServeHTTP(w, r)
		})
	}
	return next
}

func (s *Server) handleValidate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gateKeeper := s.getGateKeeper(r)
		if gateKeeper == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !gateKeeper.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		source := getSourceName(r)
		key := getKey(r)
		if !s.IsValid(source, key) {
			log.Error(fmt.Errorf("unknown source + key => %s + %s", source, key))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleError(w http.ResponseWriter, err error) {
	log.Error(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
}

func (s *Server) routeHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) routeFile(w http.ResponseWriter, r *http.Request) {
	source := getSourceName(r)
	file := r.URL.Path[len("/static/"):]
	root, err := fileutil.Clean(filepath.Join(s.ServeDir, source))
	if err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	path, err := fileutil.Clean(filepath.Join(root, file))
	if err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
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
			w.Header().Set(HeaderContentType, HeaderJSON)
			if err = s.respond(w, http.StatusOK, respJSON); err != nil {
				log.Error(err.Error())
			}
		default:
			var fp *os.File
			fp, err = os.Open(path)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer fp.Close()
			_, _ = io.Copy(w, fp)
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

func (s *Server) routePartials(w http.ResponseWriter, r *http.Request) {
	gateKeeper := s.getGateKeeper(r)
	// Starting with v1, this API is versioned in order to allow for changes to
	// the underlying structure of the partials.
	version := ""
	vers := r.URL.Query()["v"]
	if len(vers) > 0 {
		version = vers[0]
	}
	jsonBytes, err := gateKeeper.Scan(version)
	if err != nil {
		s.handleError(w, err)
		return
	}
	w.Header().Set(HeaderSep, string(os.PathSeparator))
	w.Header().Set(HeaderContentType, HeaderJSON)
	if err := s.respond(w, http.StatusOK, jsonBytes); err != nil {
		s.handleError(w, err)
	}
}

func (s *Server) routeValidate(w http.ResponseWriter, r *http.Request) {
	source := getSourceName(r)
	if source == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	sep := r.Header.Get(HeaderSep)
	files := []*confirmable{}
	var br io.Reader
	var err error
	if br, err = GetReqReader(r); err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	decoder := json.NewDecoder(br)
	err = decoder.Decode(&files)
	if source == "" || err != nil {
		if err != nil {
			log.Error(err.Error())
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	gateKeeper := s.getGateKeeper(r)
	respMap := make(map[string]int, len(files))
	for _, f := range files {
		if sep != "" {
			f.Name = filepath.Join(strings.Split(f.Name, sep)...)
		}
		respMap[f.Name] = gateKeeper.GetFileStatus(f.GetName(), f.GetStarted())
	}
	respJSON, _ := json.Marshal(respMap)
	w.Header().Set(HeaderSep, string(os.PathSeparator))
	w.Header().Set(HeaderContentType, HeaderJSON)
	if err := s.respond(w, http.StatusOK, respJSON); err != nil {
		log.Error(err.Error())
	}
}

func (s *Server) routeData(w http.ResponseWriter, r *http.Request) {
	var err error
	defer r.Body.Close()
	source := getSourceName(r)
	compressed := r.Header.Get(HeaderContentEncoding) == HeaderGzip
	sep := r.Header.Get(HeaderSep)
	var metaLen int
	if metaLen, err = strconv.Atoi(r.Header.Get(HeaderMetaLen)); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	reader := r.Body
	if compressed {
		if reader, err = gzip.NewReader(reader); err != nil {
			s.handleError(w, err)
			return
		}
	}
	decoder, err := s.DecoderFactory(metaLen, sep, reader)
	if err != nil {
		s.handleError(w, err)
		return
	}
	parts := decoder.GetParts()
	gateKeeper := s.getGateKeeper(r)
	gateKeeper.Prepare(parts)
	index := 0
	for {
		next, eof := decoder.Next()
		if eof { // Reached end of multipart request
			w.WriteHeader(http.StatusOK)
			break
		}
		if index >= len(parts) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.potentiallySimulateFailure(w, float64(index+1)/float64(len(parts)))
		file := &sts.Partial{
			Name:    parts[index].GetName(),
			Renamed: parts[index].GetRenamed(),
			Prev:    parts[index].GetPrev(),
			Size:    parts[index].GetFileSize(),
			Time:    marshal.NanoTime{Time: parts[index].GetFileTime()},
			Hash:    parts[index].GetFileHash(),
			Source:  source,
		}
		beg, end := parts[index].GetSlice()
		file.Parts = append(file.Parts, &sts.ByteRange{
			Beg: beg, End: end,
		})
		err = gateKeeper.Receive(file, next)
		if err != nil {
			log.Error(err.Error())
			w.Header().Add(HeaderPartCount, strconv.Itoa(index))
			w.WriteHeader(http.StatusPartialContent) // respond with a 206
			break
		}
		index++
	}
}

func (s *Server) routeDataRecovery(w http.ResponseWriter, r *http.Request) {
	var err error
	defer r.Body.Close()
	compressed := r.Header.Get(HeaderContentEncoding) == HeaderGzip
	sep := r.Header.Get(HeaderSep)
	reader := r.Body
	if compressed {
		if reader, err = gzip.NewReader(reader); err != nil {
			s.handleError(w, err)
			return
		}
	}
	decoder, err := s.DecoderFactory(0, sep, reader)
	if err != nil {
		s.handleError(w, err)
		return
	}
	gateKeeper := s.getGateKeeper(r)
	n := gateKeeper.Received(decoder.GetParts())
	w.Header().Add(HeaderPartCount, strconv.Itoa(n))
}

func (s *Server) routeCheckMapping(w http.ResponseWriter, r *http.Request) {
	var err error
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.handleError(w, err)
		return
	}
	type RequestBody struct {
		ExamplePath string             `json:"path"`
		Source      string             `json:"source"`
		RootPath    string             `json:"root"`
		Mapping     []*sts.MappingConf `json:"mapping"`
	}
	var reqBody RequestBody
	err = json.Unmarshal(body, &reqBody)
	if err != nil {
		s.handleError(w, err)
		return
	}
	extraVars := (&sts.SourceConf{
		Name: reqBody.Source,
	}).GenMappingVars()
	funcs := fileutil.CreateDateFuncs()
	var maps []*fileutil.PathMap
	for _, m := range reqBody.Mapping {
		maps = append(maps, &fileutil.PathMap{
			Pattern:   m.Pattern,
			Template:  m.Template,
			ExtraVars: extraVars,
			Funcs:     funcs,
		})
	}
	pathMapper := fileutil.PathMapper{
		Maps:   maps,
		Logger: log.Get(),
	}
	path := reqBody.ExamplePath
	root := strings.TrimSuffix(strings.TrimSuffix(reqBody.RootPath, "/"), `\`)
	if strings.HasPrefix(path, root) && path != root {
		path = path[len(root)+1:]
	}
	mappedPath := pathMapper.Translate(path)
	var respJSON []byte
	respJSON, err = json.Marshal(map[string]string{"mapped": mappedPath})
	if err != nil {
		s.handleError(w, err)
		return
	}
	w.Header().Set(HeaderContentType, HeaderJSON)
	if err = s.respond(w, http.StatusOK, respJSON); err != nil {
		log.Error(err.Error())
	}
}

func (s *Server) routeInternal(w http.ResponseWriter, r *http.Request) {
	wait := r.URL.Query().Has("block") || r.URL.Query().Has("wait")
	debug := r.URL.Query().Has("debug")
	minAgeSecs, err := strconv.Atoi(r.URL.Query().Get("minage"))
	if err != nil {
		minAgeSecs = 0
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	gateKeeper := s.getGateKeeper(r)
	switch r.URL.Path {
	case "/clean":
		if gateKeeper == nil || r.Method != http.MethodPut {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		go func() {
			defer wg.Done()
			name := getSourceName(r)
			log.Info(name, " -> Cleaning ...")
			defer log.Info(name, " -> Cleaned")
			if debug && !log.GetDebug() {
				log.SetDebug(true)
				defer log.SetDebug(false)
			}
			gateKeeper.CleanNow()
		}()
	case "/prune":
		if gateKeeper == nil || r.Method != http.MethodPut {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		minAge := time.Duration(minAgeSecs) * time.Second
		go func() {
			defer wg.Done()
			name := getSourceName(r)
			log.Info(name, " -> Pruning ...")
			defer log.Info(name, " -> Pruned")
			if debug && !log.GetDebug() {
				log.SetDebug(true)
				defer log.SetDebug(false)
			}
			gateKeeper.Prune(minAge)
		}()
	case "/restart":
		if gateKeeper == nil || r.Method != http.MethodPut {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		go func() {
			defer wg.Done()
			name := getSourceName(r)
			log.Info(name, " -> Restarting ...")
			defer log.Info(name, " -> Restarted")
			if debug && !log.GetDebug() {
				log.SetDebug(true)
				defer log.SetDebug(false)
			}
			gateKeeper.Stop(true)
			gateKeeper.Recover()
		}()
	case "/debug":
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.SetDebug(!log.GetDebug())
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if wait {
		wg.Wait()
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) respond(w http.ResponseWriter, status int, data []byte) error {
	if s.Compression != gzip.NoCompression {
		gz, err := gzip.NewWriterLevel(w, s.Compression)
		if err != nil {
			_, _ = w.Write(data)
			return err
		}
		w.Header().Add("Content-Encoding", "gzip")
		w.WriteHeader(status)
		defer gz.Close()
		_, _ = gz.Write(data)
		return nil
	}
	_, _ = w.Write(data)
	return nil
}

func getSourceName(r *http.Request) string {
	sourceName := r.Header.Get(HeaderSourceName)
	if sourceName == "" {
		sourceName = r.URL.Query().Get("source")
	}
	return sourceName
}

func getKey(r *http.Request) string {
	key := r.Header.Get(HeaderKey)
	if key == "" {
		key = r.URL.Query().Get("key")
	}
	return key
}

func (s *Server) potentiallySimulateFailure(w http.ResponseWriter, pctDone float64) {
	if s.ChanceOfSimulatedFailure == 0 {
		return
	}
	if s.ChanceOfSimulatedFailure*pctDone < rand.Float64() {
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	conn.Close()
}
