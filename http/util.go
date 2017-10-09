package http

import (
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
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

var gCerts map[string]*tls.Certificate
var gCertPools map[string]*x509.CertPool
var gCertLock sync.RWMutex
var gHosts map[string]string
var gHostLock sync.RWMutex

// DefaultServer is the default Server used by the functions in this
// package. It's used as to simplify using the graceful server.
var DefaultServer *GracefulServer

// ListenAndServe listens on the TCP network address addr using the
// DefaultServer. If the handler is nil, http.DefaultServeMux is used.
func ListenAndServe(addr string, tlsConf *tls.Config, handler http.Handler) error {
	h := handler
	if h == nil {
		h = http.DefaultServeMux
	}
	if DefaultServer == nil {
		DefaultServer = NewGracefulServer(&http.Server{Addr: addr, Handler: h}, tlsConf)
	}
	return DefaultServer.ListenAndServe()
}

// Close gracefully shuts down the DefaultServer.
func Close() error {
	return DefaultServer.Close()
}

// GetClient returns a secure http.Client instance pointer based on cert paths.
func GetClient(tlsConf *tls.Config) (client *http.Client, err error) {
	// Create new client using tls config
	trans := http.Transport{}
	if tlsConf != nil {
		trans.TLSClientConfig = tlsConf
	}
	trans.DisableKeepAlives = true
	client = &http.Client{}
	client.Transport = &trans
	return
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
type GracefulServer struct {
	s       *http.Server
	tlsConf *tls.Config
	wg      sync.WaitGroup
	l       net.Listener
}

// NewGracefulServer turns the given net/http server into a graceful server.
func NewGracefulServer(srv *http.Server, tlsConf *tls.Config) *GracefulServer {
	return &GracefulServer{
		s:       srv,
		tlsConf: tlsConf,
	}
}

// ListenAndServe works like net/http.Server.ListenAndServe except
// that it gracefully shuts down when Close() is called. When that
// occurs, no new connections will be allowed and existing connections
// will be allowed to finish. This will not return until all existing
// connections have closed. Will use provided tls.Config for https
// if applicable.
func (s *GracefulServer) ListenAndServe() error {
	addr := s.s.Addr
	if addr == "" {
		if s.tlsConf != nil {
			addr = ":https"
		} else {
			addr = ":http"
		}
	}
	if s.tlsConf != nil && s.tlsConf.NextProtos == nil {
		s.tlsConf.NextProtos = []string{"http/1.1"}
	}
	var err error
	var l net.Listener
	if s.tlsConf != nil {
		l, err = tls.Listen("tcp", addr, s.tlsConf)
	} else {
		l, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Serve works like ListenAndServer but using the given listener.
func (s *GracefulServer) Serve(l net.Listener) error {
	s.l = l
	err := s.s.Serve(&gracefulListener{s.l, s})
	s.wg.Wait()
	return err
}

// Close gracefully shuts down the listener. This should be called
// when the server should stop listening for new connection and finish
// any open connections.
func (s *GracefulServer) Close() error {
	// s.Shutdown() is more graceful than l.Close() (available as of Go 1.8)
	return s.s.Shutdown(nil)
}

// gracefulListener implements the net.Listener interface. When accept
// for the underlying listener returns a connection, it adds 1 to the
// servers wait group. The connection will be a gracefulConn which
// will call Done() when it finished.
type gracefulListener struct {
	net.Listener
	s *GracefulServer
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
	s    *GracefulServer
	once sync.Once
}

func (g *gracefulConn) Close() error {
	err := g.Conn.Close()
	g.once.Do(g.s.wg.Done)
	return err
}

// GetTLSConf returns a tls.Config reference based on provided paths.
func GetTLSConf(certPath, keyPath, caPath string) (conf *tls.Config, err error) {
	if certPath == "" && keyPath == "" && caPath == "" {
		return
	}
	conf = &tls.Config{}
	var cert *tls.Certificate
	var pool *x509.CertPool
	if cert, pool, err = loadCert(certPath, keyPath, caPath); err != nil {
		return
	}
	if cert != nil {
		conf.Certificates = []tls.Certificate{*cert}
	}
	if pool != nil {
		conf.RootCAs = pool
		conf.ClientCAs = pool
	}
	// conf.ClientAuth = tls.RequireAndVerifyClientCert
	conf.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}

	conf.MinVersion = tls.VersionTLS12
	conf.SessionTicketsDisabled = true
	return
}

func loadCert(certPath, keyPath, caPath string) (pem *tls.Certificate, pool *x509.CertPool, err error) {
	gCertLock.Lock()
	defer gCertLock.Unlock()
	key := strings.Join([]string{certPath, keyPath, caPath}, ":")
	if gCerts == nil {
		gCerts = make(map[string]*tls.Certificate)
	}
	if gCertPools == nil {
		gCertPools = make(map[string]*x509.CertPool)
	}
	var ok bool
	if certPath != "" && keyPath != "" {
		if pem, ok = gCerts[key]; !ok {
			var p tls.Certificate
			p, err = tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				return
			}
			pem = &p
			gCerts[key] = pem
		}
	}
	if caPath != "" {
		if pool, ok = gCertPools[key]; !ok {
			var ca []byte
			ca, err = ioutil.ReadFile(caPath)
			if err != nil {
				return
			}
			pool = x509.NewCertPool()
			pool.AppendCertsFromPEM(ca)
		}
	}
	return
}
