package httputils

import (
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
)

const (
	// HeaderSourceName is the custom HTTP header for communicating the name of
	// the sending host.
	HeaderSourceName = "X-STS-SrcName"

	// HeaderKey is the custom HTTP header for communicating the key (if applicable)
	// provided by the target.
	HeaderKey = "X-STS-Key"

	// HeaderMetaLen is the custom HTTP header that houses the JSON-encoded metadata
	// length for a bin.
	HeaderMetaLen = "X-STS-MetaLen"

	// HeaderPartCount is the custom HTTP resopnse header that indicates the number
	// of parts successfully received.
	HeaderPartCount = "X-STS-PartCount"

	// HeaderSep is the custom HTTP header used to indicate the path separator string
	// to be used for parsing paths in request/response data.
	HeaderSep = "X-STS-Sep"

	// HeaderContentType is the standard HTTP header for specifying the payload
	// type.
	HeaderContentType = "Content-Type"

	// HeaderContentEncoding is the standard HTTP header for specifying the payload
	// encoding.
	HeaderContentEncoding = "Content-Encoding"

	// HeaderGzip is the content encoding value for gzip compression.
	HeaderGzip = "gzip"

	// HeaderJSON is the content type value for JSON-encoded payload.
	HeaderJSON = "application/json"
)

var gCerts map[string]tls.Certificate
var gCertLock sync.RWMutex
var gHosts map[string]string
var gHostLock sync.RWMutex

// DefaultServer is the default Server used by the functions in this
// package. It's used as to simplify using the graceful server.
var DefaultServer *Server

// ListenAndServe listens on the TCP network address addr using the
// DefaultServer. If the handler is nil, http.DefaultServeMux is used.
func ListenAndServe(addr string, handler http.Handler) error {
	h := handler
	if h == nil {
		h = http.DefaultServeMux
	}
	if DefaultServer == nil {
		DefaultServer = NewServer(&http.Server{Addr: addr, Handler: h})
	}
	return DefaultServer.ListenAndServe()
}

// ListenAndServeTLS acts identically to ListenAndServe except that is
// expects HTTPS connections using the given certificate and key.
func ListenAndServeTLS(addr, certFile, keyFile string, handler http.Handler) error {
	h := handler
	if h == nil {
		h = http.DefaultServeMux
	}
	if DefaultServer == nil {
		DefaultServer = NewServer(&http.Server{Addr: addr, Handler: h})
	}
	return DefaultServer.ListenAndServeTLS(certFile, keyFile)
}

// Close gracefully shuts down the DefaultServer.
func Close() error {
	return DefaultServer.Close()
}

// GetClient returns a secure http.Client instance pointer based on cert paths.
func GetClient(clientCertPath, clientKeyPath string) (*http.Client, error) {
	var tlsConfig tls.Config
	if clientCertPath != "" && clientKeyPath != "" {
		cert, err := loadCert(clientCertPath, clientKeyPath)
		if err != nil {
			return &http.Client{}, err
		}
		tlsConfig = tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, // TODO: Get real certs.
		}
	} else {
		tlsConfig = tls.Config{}
	}
	// Create new client using tls config
	trans := http.Transport{}
	trans.TLSClientConfig = &tlsConfig
	trans.DisableKeepAlives = true
	client := &http.Client{}
	client.Transport = &trans
	return client, nil
}

// GetHostname uses an http.Request object to determine the name of the sending host.
// Falls back to the IP address.
func GetHostname(r *http.Request) string {
	gHostLock.Lock()
	defer gHostLock.Unlock()
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if gHosts == nil {
		gHosts = make(map[string]string)
	}
	memoizedName, memoized := gHosts[ip]
	if memoized {
		return memoizedName
	}
	hosts, err := net.LookupAddr(ip)
	if err != nil || len(hosts) < 1 {
		return ip
	}
	host := strings.TrimSuffix(hosts[0], ".")
	gHosts[ip] = host
	return host
}

// GetRespReader will return the appropriate reader for this response body based
// on the content type.
func GetRespReader(r *http.Response) (io.ReadCloser, error) {
	if r.Header.Get(HeaderContentEncoding) == HeaderGzip {
		return gzip.NewReader(r.Body)
	}
	return r.Body, nil
}

// GetReqReader will return the appropriate reader for this request body based
// on the content type.
func GetReqReader(r *http.Request) (io.ReadCloser, error) {
	if r.Header.Get(HeaderContentEncoding) == HeaderGzip {
		return gzip.NewReader(r.Body)
	}
	return r.Body, nil
}

// GetJSONReader takes an arbitrary struct and generates a JSON reader with no
// intermediate buffer.  It will also add gzip compression at specified level.
func GetJSONReader(data interface{}, compression int) (r io.Reader, err error) {
	rp, wp := io.Pipe()
	var wz *gzip.Writer
	var wj *json.Encoder
	if compression != gzip.NoCompression {
		wz, err = gzip.NewWriterLevel(wp, compression)
		if err != nil {
			return
		}
		wj = json.NewEncoder(wz)
	} else {
		wj = json.NewEncoder(wp)
	}
	go func(d interface{}) {
		wj.Encode(d)
		if wz != nil {
			wz.Close()
		}
		wp.Close()
	}(data)
	return rp, nil
}

// Server is net/http compatible graceful server.
// https://github.com/icub3d/graceful/blob/master/graceful.go
type Server struct {
	s  *http.Server
	wg sync.WaitGroup
	l  net.Listener
}

// NewServer turns the given net/http server into a graceful server.
func NewServer(srv *http.Server) *Server {
	return &Server{
		s: srv,
	}
}

// ListenAndServe works like net/http.Server.ListenAndServe except
// that it gracefully shuts down when Close() is called. When that
// occurs, no new connections will be allowed and existing connections
// will be allowed to finish. This will not return until all existing
// connections have closed.
func (s *Server) ListenAndServe() error {
	addr := s.s.Addr
	if addr == "" {
		addr = ":http"
	}
	var err error
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// ListenAndServeTLS works like ListenAndServe but with TLS using the
// given cert and key files.
func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	addr := s.s.Addr
	if addr == "" {
		addr = ":https"
	}
	config := &tls.Config{}
	if s.s.TLSConfig != nil {
		*config = *s.s.TLSConfig
	}
	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1"}
	}
	cert, err := loadCert(certFile, keyFile)
	if err != nil {
		return err
	}
	config.Certificates = []tls.Certificate{cert}
	config.InsecureSkipVerify = true // TODO: Get real certs.
	l, err := tls.Listen("tcp", addr, config)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Serve works like ListenAndServer but using the given listener.
func (s *Server) Serve(l net.Listener) error {
	s.l = l
	err := s.s.Serve(&gracefulListener{s.l, s})
	s.wg.Wait()
	return err
}

// Close gracefully shuts down the listener. This should be called
// when the server should stop listening for new connection and finish
// any open connections.
func (s *Server) Close() error {
	err := s.l.Close()
	return err
}

// gracefulListener implements the net.Listener interface. When accept
// for the underlying listener returns a connection, it adds 1 to the
// servers wait group. The connection will be a gracefulConn which
// will call Done() when it finished.
type gracefulListener struct {
	net.Listener
	s *Server
}

func (g *gracefulListener) Accept() (net.Conn, error) {
	c, err := g.Listener.Accept()
	if err != nil {
		return nil, err
	}
	g.s.wg.Add(1)
	return &gracefulConn{Conn: c, s: g.s}, nil
}

// gracefulConn implements the net.Conn interface. When it closes, it
// calls Done() on the servers waitgroup.
type gracefulConn struct {
	net.Conn
	s    *Server
	once sync.Once
}

func (g *gracefulConn) Close() error {
	err := g.Conn.Close()
	g.once.Do(g.s.wg.Done)
	return err
}

func loadCert(certPath string, keyPath string) (tls.Certificate, error) {
	gCertLock.Lock()
	defer gCertLock.Unlock()
	certKey := getCertKey(certPath, keyPath)
	if gCerts == nil {
		gCerts = make(map[string]tls.Certificate)
	}
	cert, exists := gCerts[certKey]
	if !exists {
		// Load cert from disk
		_, certErr := os.Stat(certPath)
		_, keyErr := os.Stat(keyPath)
		if os.IsNotExist(certErr) {
			return tls.Certificate{}, certErr
		}
		if os.IsNotExist(keyErr) {
			return tls.Certificate{}, keyErr
		}
		var err error
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return tls.Certificate{}, err
		}
		gCerts[certKey] = cert
	}
	return cert, nil
}

func getCertKey(cert string, key string) string {
	return cert + ":" + key
}
