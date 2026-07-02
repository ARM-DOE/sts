package http

import (
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/fileutil"
	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/marshal"
	"github.com/quic-go/quic-go"
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

	// HTTP3/QUIC support
	EnableHTTP3 bool
	HTTP3Port   int // 0 means use same port as HTTPS
	QUICConfig  *quic.Config

	GateKeepers       map[string]sts.GateKeeper
	GateKeeperFactory sts.GateKeeperFactory
	DecoderFactory    sts.PayloadDecoderFactory

	IsValid       sts.RequestValidator
	ClientManager sts.ClientManager

	ChanceOfSimulatedFailure float64

	lock sync.RWMutex
}

var safePathSegmentRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

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

func hasRequestBody(r *http.Request) bool {
	return r.ContentLength != 0 || len(r.TransferEncoding) > 0
}

func normalizeRepeatedSlashes(path string) string {
	if path == "" {
		return "/"
	}
	for strings.Contains(path, "//") {
		path = strings.ReplaceAll(path, "//", "/")
	}
	return path
}

func normalizeRequestPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cleanPath := normalizeRepeatedSlashes(r.URL.Path)
		cleanRawPath := normalizeRepeatedSlashes(r.URL.RawPath)
		if cleanPath == r.URL.Path && cleanRawPath == r.URL.RawPath {
			next.ServeHTTP(w, r)
			return
		}
		r2 := r.Clone(r.Context())
		r2.URL.Path = cleanPath
		r2.URL.RawPath = cleanRawPath
		next.ServeHTTP(w, r2)
	})
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
	// Create a shared ServeMux for both HTTP/1.1 and HTTP3 servers
	mux := http.NewServeMux()
	mux.Handle(s.PathPrefix+"/", s.handle(http.HandlerFunc(s.routeHealthCheck)))
	mux.Handle(s.PathPrefix+"/client/", s.handle(http.HandlerFunc(s.routeClientManagement)))
	mux.Handle(s.PathPrefix+"/data", s.handle(s.handleValidate(http.HandlerFunc(s.routeData))))
	mux.Handle(s.PathPrefix+"/data-recovery", s.handle(s.handleValidate(http.HandlerFunc(s.routeDataRecovery))))
	mux.Handle(s.PathPrefix+"/validate", s.handle(s.handleValidate(http.HandlerFunc(s.routeValidate))))
	mux.Handle(s.PathPrefix+"/partials", s.handle(s.handleValidate(http.HandlerFunc(s.routePartials))))
	mux.Handle(s.PathPrefix+"/static/", s.handle(s.handleValidate(http.HandlerFunc(s.routeFile))))
	mux.Handle(s.PathPrefix+"/check-mapping", s.handle(http.HandlerFunc(s.routeCheckMapping)))
	handler := normalizeRequestPath(mux)

	wg := sync.WaitGroup{}

	// Start HTTP/1.1 server
	wg.Add(1)
	go func() {
		defer wg.Done()
		addr := fmt.Sprintf("%s:%d", s.Host, s.Port)
		err := ListenAndServe(addr, s.TLS, handler)
		if err != http.ErrServerClosed {
			log.Error("Error shutting down HTTP/1.1 server:", err.Error())
		}
		s.stopGateKeepers()
	}()

	// Start HTTP3 server if enabled
	var http3Server *HTTP3Server
	if s.EnableHTTP3 && s.TLS != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			http3Port := s.HTTP3Port
			if http3Port == 0 {
				http3Port = s.Port // Use same port as HTTPS
			}
			addr := fmt.Sprintf("%s:%d", s.Host, http3Port)

			http3Server = NewHTTP3Server(addr, handler, s.TLS, s.QUICConfig)
			err := http3Server.ListenAndServe()
			if err != nil {
				log.Error("Error shutting down HTTP3 server:", err.Error())
			}
		}()
	}

	// Start internal server (keep as HTTP/1.1 for simplicity)
	iServer := NewGracefulServer(&http.Server{
		Addr:    fmt.Sprintf(":%d", s.Port+1),
		Handler: http.HandlerFunc(s.routeInternal),
	}, nil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := iServer.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Error("Error shutting down internal server:", err.Error())
		}
	}()

	<-stop

	// Graceful shutdown
	_ = Close() // HTTP/1.1 server
	if http3Server != nil {
		_ = http3Server.CloseGracefully(30 * time.Second)
	}
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
		log.Debug(
			"STS request validate:",
			r.Method,
			r.URL.RequestURI(),
			"remote=", r.RemoteAddr,
			"source=", getSourceName(r),
			"key=", getKey(r),
		)
		gateKeeper := s.getGateKeeper(r)
		if gateKeeper == nil {
			log.Debug("STS request rejected: missing gatekeeper")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !gateKeeper.Ready() {
			log.Debug("STS request rejected: gatekeeper not ready")
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
		log.Debug("STS request validated:", r.Method, r.URL.RequestURI(), "source=", source)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleError(w http.ResponseWriter, err error) {
	log.Error(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
}

func (s *Server) routeHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) routeFile(w http.ResponseWriter, r *http.Request) {
	source, err := sanitizePathSegment(getSourceName(r))
	if err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file, err := sanitizeRelativePath(r.URL.Path[len("/static/"):])
	if err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	serveRoot, err := fileutil.Clean(s.ServeDir)
	if err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	root, err := fileutil.Clean(filepath.Join(serveRoot, source))
	if err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !isSubpath(serveRoot, root) {
		log.Error(fmt.Errorf("invalid source path outside serve root: %s", source))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	path, err := fileutil.Clean(filepath.Join(root, file))
	if err != nil {
		log.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !isSubpath(root, path) {
		log.Error(fmt.Errorf("invalid file path outside source root: %s", file))
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
			handleNode := func(nodePath string, d fs.DirEntry, err error) error {
				if d == nil || err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				names = append(names, nodePath[len(root)+1:])
				return nil
			}
			if err = filepath.WalkDir(path, handleNode); err != nil {
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

func isSubpath(base, target string) bool {
	base = strings.TrimRight(base, string(os.PathSeparator)) + string(os.PathSeparator)
	target = strings.TrimRight(target, string(os.PathSeparator)) + string(os.PathSeparator)
	return strings.HasPrefix(target, base)
}

func sanitizePathSegment(value string) (string, error) {
	if value == "" || !safePathSegmentRe.MatchString(value) {
		return "", fmt.Errorf("invalid path segment: %s", value)
	}
	return value, nil
}

func sanitizeRelativePath(value string) (string, error) {
	raw := strings.Trim(value, "/")
	if raw == "" {
		return "", nil
	}
	for _, segment := range strings.Split(raw, "/") {
		switch segment {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("invalid path segment: %s", segment)
		}
		if _, err := sanitizePathSegment(segment); err != nil {
			return "", err
		}
	}
	cleaned := strings.TrimPrefix(path.Clean("/"+raw), "/")
	if cleaned == "." {
		return "", nil
	}
	if cleaned == "" {
		return "", nil
	}
	return cleaned, nil
}

func (s *Server) routePartials(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
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
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !hasRequestBody(r) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
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
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !hasRequestBody(r) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	source := getSourceName(r)
	compressed := r.Header.Get(HeaderContentEncoding) == HeaderGzip
	sep := r.Header.Get(HeaderSep)
	var metaLen int
	if metaLen, err = strconv.Atoi(r.Header.Get(HeaderMetaLen)); err != nil {
		log.Debug("STS data request rejected: invalid meta len", r.Header.Get(HeaderMetaLen))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debug(
		"STS data request start:",
		"source=", source,
		"remote=", r.RemoteAddr,
		"compressed=", compressed,
		"metaLen=", metaLen,
		"contentLength=", r.ContentLength,
	)
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
			log.Debug("STS data request complete:", "source=", source, "parts=", index)
			w.WriteHeader(http.StatusOK)
			break
		}
		if index >= len(parts) {
			log.Debug("STS data request rejected: part index overflow", index, len(parts))
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
			log.Debug("STS data request partial failure:", "source=", source, "partsReceived=", index)
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
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !hasRequestBody(r) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	compressed := r.Header.Get(HeaderContentEncoding) == HeaderGzip
	sep := r.Header.Get(HeaderSep)
	source := getSourceName(r)
	log.Debug(
		"STS data-recovery request start:",
		"source=", source,
		"remote=", r.RemoteAddr,
		"compressed=", compressed,
		"contentLength=", r.ContentLength,
	)
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
	parts := decoder.GetParts()
	n := gateKeeper.Received(parts)
	log.Debug("STS data-recovery request complete:", "source=", source, "partsReceived=", n)
	w.Header().Add(HeaderPartCount, strconv.Itoa(n))
}

func (s *Server) routeCheckMapping(w http.ResponseWriter, r *http.Request) {
	var err error
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !hasRequestBody(r) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
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
