package http

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arm-doe/sts/fileutil"
	"github.com/arm-doe/sts/log"
	"github.com/alecthomas/units"
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

	// readTimeout is the server timeout for request reads.
	readTimeout = time.Minute * 30

	// writeTimeout is the server timeout for writing a response.
	writeTimeout = time.Minute * 30
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
		DefaultServer = NewGracefulServer(
			&http.Server{
				Addr:         addr,
				Handler:      h,
				ReadTimeout:  readTimeout,
				WriteTimeout: writeTimeout,
			}, tlsConf)
	}
	return DefaultServer.ListenAndServe()
}

// Close gracefully shuts down the DefaultServer.
func Close() error {
	if DefaultServer == nil {
		return nil
	}
	return DefaultServer.Close()
}

// GetClient returns a secure http.Client instance pointer based on cert paths.
func GetClient(
	tlsConf *tls.Config,
	logInterval time.Duration,
	logPrefix string,
) (client *BandwidthLoggingClient, err error) {
	// Create new client using tls config
	// NOTE: cloning the default transport means we get things like honoring
	// proxy env vars (e.g. HTTP_PROXY, HTTPS_PROXY, etc)...
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tlsConf != nil {
		tr.TLSClientConfig = tlsConf
	}
	tr.DisableKeepAlives = true
	client = newBandwidthLoggingClient(tr, logInterval, func(i ...interface{}) {
		log.Info(append([]interface{}{logPrefix}, i...)...)
	})
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

// GracefulServer is net/http compatible graceful server.
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
	return s.s.Shutdown(context.Background())
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

// LoadTLSConf returns a tls.Config reference based on provided paths.
func LoadTLSConf(certPath, keyPath, caPath string) (conf *tls.Config, err error) {
	if certPath == "" && keyPath == "" && caPath == "" {
		return
	}
	var cert *tls.Certificate
	var pool *x509.CertPool
	if cert, pool, err = loadCert(certPath, keyPath, caPath); err != nil {
		return
	}
	conf, err = getTLSConf(cert, pool)
	return
}

// TLSConf returns a tls.Config reference based on provided strings.
func TLSConf(cert, key, ca []byte) (conf *tls.Config, err error) {
	if len(cert) > 0 && len(key) > 0 && len(ca) > 0 {
		return
	}
	var pem *tls.Certificate
	var pool *x509.CertPool
	if pem, pool, err = getCert(cert, key, ca); err != nil {
		return
	}
	conf, err = getTLSConf(pem, pool)
	return
}

func getTLSConf(pem *tls.Certificate, pool *x509.CertPool) (conf *tls.Config, err error) {
	conf = &tls.Config{}
	if pem != nil {
		conf.Certificates = []tls.Certificate{*pem}
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
			ca, err = os.ReadFile(caPath)
			if err != nil {
				return
			}
			pool = x509.NewCertPool()
			pool.AppendCertsFromPEM(ca)
		}
	}
	return
}

func getCert(cert, key, ca []byte) (pem *tls.Certificate, pool *x509.CertPool, err error) {
	id := fileutil.StringMD5(string(cert) + string(key) + string(ca))
	gCertLock.Lock()
	defer gCertLock.Unlock()
	if gCerts == nil {
		gCerts = make(map[string]*tls.Certificate)
	}
	if gCertPools == nil {
		gCertPools = make(map[string]*x509.CertPool)
	}
	var ok bool
	if len(cert) > 0 && len(key) > 0 {
		if pem, ok = gCerts[id]; !ok {
			var p tls.Certificate
			p, err = tls.X509KeyPair(cert, key)
			if err != nil {
				return
			}
			pem = &p
			gCerts[id] = pem
		}
	}
	if len(ca) > 0 {
		if pool, ok = gCertPools[id]; !ok {
			pool = x509.NewCertPool()
			pool.AppendCertsFromPEM(ca)
		}
	}
	return
}

func handleError(w http.ResponseWriter, code int, err error) {
	log.Error(err.Error())
	w.WriteHeader(code)
	w.Write([]byte(err.Error()))
}

type bandwidthMonitorTransport struct {
	transport     http.RoundTripper
	bytesSent     uint64
	bytesReceived uint64
	totalDuration uint64 // in nanoseconds
}

func (t *bandwidthMonitorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.transport == nil {
		t.transport = http.DefaultTransport
	}

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		atomic.AddUint64(&t.totalDuration, uint64(duration))
	}()

	if req.Body != nil {
		originalBody := req.Body
		req.Body = &readCounter{req.Body, &t.bytesSent}
		defer originalBody.Close()
	}

	resp, err := t.transport.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	resp.Body = &readCounter{resp.Body, &t.bytesReceived}
	return resp, nil
}

func (t *bandwidthMonitorTransport) GetBytesSent() uint64 {
	return atomic.LoadUint64(&t.bytesSent)
}

func (t *bandwidthMonitorTransport) GetBytesReceived() uint64 {
	return atomic.LoadUint64(&t.bytesReceived)
}

func (t *bandwidthMonitorTransport) GetTotalDuration() time.Duration {
	return time.Duration(atomic.LoadUint64(&t.totalDuration))
}

func (t *bandwidthMonitorTransport) ResetCounters() {
	atomic.StoreUint64(&t.bytesSent, 0)
	atomic.StoreUint64(&t.bytesReceived, 0)
	atomic.StoreUint64(&t.totalDuration, 0)
}

type readCounter struct {
	reader  io.Reader
	counter *uint64
}

func (r *readCounter) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	atomic.AddUint64(r.counter, uint64(n))
	return
}

func (r *readCounter) Close() error {
	if c, ok := r.reader.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

type BandwidthLoggingClient struct {
	*http.Client
	monitor  *bandwidthMonitorTransport
	stopChan chan struct{}
}

func newBandwidthLoggingClient(
	baseTransport http.RoundTripper,
	logInterval time.Duration,
	logFn func(...interface{}),
) *BandwidthLoggingClient {
	monitor := &bandwidthMonitorTransport{transport: baseTransport}
	client := &http.Client{Transport: monitor}
	blc := &BandwidthLoggingClient{
		Client:   client,
		monitor:  monitor,
		stopChan: make(chan struct{}),
	}
	if logInterval > 0 {
		go blc.logBandwidthUsage(logInterval, logFn)
	}
	return blc
}

func (blc *BandwidthLoggingClient) Stop() {
	close(blc.stopChan)
}

func (blc *BandwidthLoggingClient) logBandwidthUsage(
	interval time.Duration,
	logFn func(...interface{}),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sent := blc.monitor.GetBytesSent()
			received := blc.monitor.GetBytesReceived()
			duration := blc.monitor.GetTotalDuration()

			if duration > 0 {
				sentBps := int64(float64(sent) / duration.Seconds())
				receivedBps := int64(float64(received) / duration.Seconds())

				lines := []string{
					fmt.Sprintf("Raw bandwidth usage in the last %s:", interval),
					fmt.Sprintf("  - Sent: %s (%s/s)", units.Base2Bytes(sent), units.Base2Bytes(sentBps)),
					fmt.Sprintf("  - Received: %s (%s/s)", units.Base2Bytes(received), units.Base2Bytes(receivedBps)),
					fmt.Sprintf("  - Total duration: %s", duration),
				}
				logFn(strings.Join(lines, "\n"))
			} else {
				logFn(fmt.Sprintf("No requests made in the last %s", interval))
			}
			// sent := blc.monitor.GetBytesSent()
			// received := blc.monitor.GetBytesReceived()
			// logFn("Bandwidth usage in the last %s - Sent: %s, Received: %s",
			// 	interval, units.Base2Bytes(sent), units.Base2Bytes(received))
			blc.monitor.ResetCounters()
		case <-blc.stopChan:
			return
		}
	}
}
