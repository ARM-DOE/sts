package http

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

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/log"
)

// Server responds to client HTTP requests
type Server struct {
	ServeDir    string
	Host        string
	Port        int
	TLS         *tls.Config
	Sources     []string
	Keys        []string
	Compression int

	DecoderFactory sts.PayloadDecoderFactory
	GateKeeper     sts.GateKeeper
}

func (s *Server) isValid(src, key string) bool {
	if src != "" {
		if matched, err := regexp.MatchString(`^[a-z0-9\.\-]+$`, src); err != nil || !matched {
			return false
		}
		if len(s.Sources) > 0 && strToIndex(src, s.Sources) < 0 {
			return false
		}
	}
	if key != "" && len(s.Keys) > 0 && strToIndex(key, s.Keys) < 0 {
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

// Serve starts HTTP server.
func (s *Server) Serve(stop <-chan bool, done chan<- bool) {
	http.Handle("/data", s.handleValidate(http.HandlerFunc(s.routeData)))
	http.Handle("/validate", s.handleValidate(http.HandlerFunc(s.routeValidate)))
	http.Handle("/partials", s.handleValidate(http.HandlerFunc(s.routePartials)))
	http.Handle("/static/", s.handleValidate(http.HandlerFunc(s.routeFile)))

	go func(done chan<- bool, host string, port int, tlsConf *tls.Config) {
		addr := fmt.Sprintf("%s:%d", host, port)
		err := ListenAndServe(addr, tlsConf, nil)
		if err != http.ErrServerClosed {
			log.Error(err.Error())
		}
		done <- true
	}(done, s.Host, s.Port, s.TLS)
	<-stop
	Close()
}

func (s *Server) handleValidate(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		source := r.Header.Get(HeaderSourceName)
		key := r.Header.Get(HeaderKey)
		if !s.isValid(source, key) {
			log.Error(fmt.Errorf("Unknown Source:Key => %s:%s", source, key))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func (s *Server) handleError(w http.ResponseWriter, err error) {
	log.Error(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
}

func (s *Server) routeFile(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(HeaderSourceName)
	if source == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file := r.URL.Path[len("/static/"):]
	root := filepath.Join(s.ServeDir, source)
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

func (s *Server) routePartials(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(HeaderSourceName)
	if source == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	partials, err := s.GateKeeper.Scan(source)
	if err != nil {
		s.handleError(w, err)
		return
	}
	respJSON, err := json.Marshal(partials)
	if err != nil {
		s.handleError(w, err)
		return
	}
	w.Header().Set(HeaderSep, string(os.PathSeparator))
	w.Header().Set(HeaderContentType, HeaderJSON)
	if err := s.respond(w, http.StatusOK, respJSON); err != nil {
		s.handleError(w, err)
	}
}

func (s *Server) routeValidate(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get(HeaderSourceName)
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
	respMap := make(map[string]int, len(files))
	for _, f := range files {
		if sep != "" {
			f.Name = filepath.Join(strings.Split(f.Name, sep)...)
		}
		respMap[f.Name] = s.GateKeeper.GetFileStatus(
			source, f.GetName(), f.GetStarted())
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
	source := r.Header.Get(HeaderSourceName)
	if source == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
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
	decoder, err := s.DecoderFactory(metaLen, reader)
	if err != nil {
		s.handleError(w, err)
		return
	}
	parts := decoder.GetParts()
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
		file := &sts.Partial{
			Name:   parts[index].GetName(),
			Prev:   parts[index].GetPrev(),
			Size:   parts[index].GetFileSize(),
			Hash:   parts[index].GetFileHash(),
			Source: source,
		}
		if sep != "" {
			file.Name = filepath.Join(strings.Split(file.Name, sep)...)
			file.Prev = filepath.Join(strings.Split(file.Prev, sep)...)
		}
		beg, end := parts[index].GetSlice()
		file.Parts = append(file.Parts, &sts.ByteRange{
			Beg: beg, End: end,
		})
		err = s.GateKeeper.Receive(file, next)
		if err != nil {
			log.Error(err.Error())
			w.Header().Add(HeaderPartCount, strconv.Itoa(len(parts)))
			w.WriteHeader(http.StatusPartialContent) // respond with a 206
			break
		}
		index++
	}
}

func (s *Server) respond(w http.ResponseWriter, status int, data []byte) error {
	if s.Compression != gzip.NoCompression {
		gz, err := gzip.NewWriterLevel(w, s.Compression)
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