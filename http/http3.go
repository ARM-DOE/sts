package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/arm-doe/sts/log"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Default QUIC configuration values
const (
	DefaultQUICMaxStreams           int64         = 100
	DefaultQUICMaxIdleTimeout       time.Duration = 30 * time.Second
	DefaultQUICKeepAlive            time.Duration = 15 * time.Second
	DefaultQUICMaxStreamReceiveWindow      uint64 = 6 * (1 << 20)  // 6 MB
	DefaultQUICMaxConnectionReceiveWindow  uint64 = 15 * (1 << 20) // 15 MB
)

// buildHTTP3Client creates an HTTP3-capable client with QUIC transport
func (tb *TransportBuilder) buildHTTP3Client() (*BandwidthLoggingClient, error) {
	if tb.TLSConfig == nil {
		return nil, fmt.Errorf("HTTP3 requires TLS configuration")
	}

	// Clone and configure TLS for HTTP3
	tlsConfig := tb.TLSConfig.Clone()
	tlsConfig.MinVersion = tls.VersionTLS13

	// Set ALPN for HTTP3
	if len(tlsConfig.NextProtos) == 0 {
		tlsConfig.NextProtos = []string{"h3"}
	} else {
		// Prepend h3 if not already present
		hasH3 := false
		for _, proto := range tlsConfig.NextProtos {
			if proto == "h3" {
				hasH3 = true
				break
			}
		}
		if !hasH3 {
			tlsConfig.NextProtos = append([]string{"h3"}, tlsConfig.NextProtos...)
		}
	}

	// Use provided QUIC config or create default
	quicConfig := tb.QUICConfig
	if quicConfig == nil {
		// Default QUIC configuration using constants
		quicConfig = &quic.Config{
			MaxIdleTimeout:             DefaultQUICMaxIdleTimeout,
			MaxIncomingStreams:         DefaultQUICMaxStreams,
			MaxIncomingUniStreams:      DefaultQUICMaxStreams,
			MaxStreamReceiveWindow:     DefaultQUICMaxStreamReceiveWindow,
			MaxConnectionReceiveWindow: DefaultQUICMaxConnectionReceiveWindow,
			KeepAlivePeriod:            DefaultQUICKeepAlive,
			EnableDatagrams:            false,
			DisablePathMTUDiscovery:    false,
			Allow0RTT:                  true, // Enable 0-RTT for performance
		}
	}

	// Create HTTP3 Transport
	transport := &http3.Transport{
		TLSClientConfig: tlsConfig,
		QUICConfig:      quicConfig,
	}

	// Wrap with bandwidth monitor
	monitor := &bandwidthMonitorTransport{transport: transport}
	client := newBandwidthLoggingClient(monitor, tb.LogInterval, func(i ...interface{}) {
		log.Info(append([]interface{}{tb.LogPrefix}, i...)...)
	})

	return client, nil
}

// HTTP3Server wraps http3.Server for graceful shutdown and configuration
type HTTP3Server struct {
	server     *http3.Server
	handler    http.Handler
	tlsConfig  *tls.Config
	quicConfig *quic.Config
	addr       string
}

// NewHTTP3Server creates a new HTTP3 server instance
func NewHTTP3Server(addr string, handler http.Handler, tlsConfig *tls.Config, quicConfig *quic.Config) *HTTP3Server {
	if tlsConfig != nil {
		tlsConfig = tlsConfig.Clone()
		tlsConfig.MinVersion = tls.VersionTLS13

		// Set ALPN for HTTP3
		if len(tlsConfig.NextProtos) == 0 {
			tlsConfig.NextProtos = []string{"h3"}
		} else {
			// Prepend h3 if not already present
			hasH3 := false
			for _, proto := range tlsConfig.NextProtos {
				if proto == "h3" {
					hasH3 = true
					break
				}
			}
			if !hasH3 {
				tlsConfig.NextProtos = append([]string{"h3"}, tlsConfig.NextProtos...)
			}
		}
	}

	if quicConfig == nil {
		quicConfig = &quic.Config{
			MaxIdleTimeout:          30 * time.Second,
			MaxIncomingStreams:      100,
			MaxIncomingUniStreams:   100,
			KeepAlivePeriod:         15 * time.Second,
			EnableDatagrams:         false,
			DisablePathMTUDiscovery: false,
			Allow0RTT:               true,
		}
	}

	server := &http3.Server{
		Addr:       addr,
		Handler:    handler,
		TLSConfig:  tlsConfig,
		QUICConfig: quicConfig,
	}

	return &HTTP3Server{
		server:     server,
		handler:    handler,
		tlsConfig:  tlsConfig,
		quicConfig: quicConfig,
		addr:       addr,
	}
}

// ListenAndServe starts the HTTP3 server
func (s *HTTP3Server) ListenAndServe() error {
	log.Info(fmt.Sprintf("Starting HTTP3/QUIC server on %s", s.addr))
	return s.server.ListenAndServe()
}

// Close immediately closes the HTTP3 server
func (s *HTTP3Server) Close() error {
	log.Info("Closing HTTP3 server")
	return s.server.Close()
}

// CloseGracefully gracefully shuts down the HTTP3 server with a timeout
func (s *HTTP3Server) CloseGracefully(timeout time.Duration) error {
	log.Info(fmt.Sprintf("Gracefully shutting down HTTP3 server (timeout: %s)", timeout))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// http3.Server uses Shutdown method, not CloseGracefully
	if err := s.server.Shutdown(ctx); err != nil {
		// If graceful shutdown fails, force close
		return s.server.Close()
	}
	return nil
}

// ConfigureQUICFromHTTPServer configures QUIC parameters from HTTPServer config.
// All parameters should be pre-computed with defaults by the caller.
func ConfigureQUICFromHTTPServer(
	conf *quic.Config,
	maxStreams int64,
	maxIdleTimeout, keepAlive time.Duration,
	maxStreamReceiveWindow, maxConnectionReceiveWindow uint64,
	enableDatagrams bool,
	disablePathMTUDiscovery bool,
) *quic.Config {
	if conf == nil {
		conf = &quic.Config{}
	}

	conf.MaxIncomingStreams = maxStreams
	conf.MaxIncomingUniStreams = maxStreams
	conf.MaxIdleTimeout = maxIdleTimeout
	conf.KeepAlivePeriod = keepAlive
	conf.MaxStreamReceiveWindow = maxStreamReceiveWindow
	conf.MaxConnectionReceiveWindow = maxConnectionReceiveWindow
	conf.EnableDatagrams = enableDatagrams
	conf.DisablePathMTUDiscovery = disablePathMTUDiscovery
	conf.Allow0RTT = true // Server always allows 0-RTT

	return conf
}

// ConfigureQUICForClient configures QUIC parameters for client.
// All parameters should be pre-computed with defaults by the caller.
func ConfigureQUICForClient(
	maxStreams int64,
	maxIdleTimeout, keepAlive time.Duration,
	maxStreamReceiveWindow, maxConnectionReceiveWindow uint64,
	enableDatagrams bool,
	disablePathMTUDiscovery bool,
	disable0RTT bool,
) *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:         maxStreams,
		MaxIncomingUniStreams:      maxStreams,
		MaxIdleTimeout:             maxIdleTimeout,
		KeepAlivePeriod:            keepAlive,
		MaxStreamReceiveWindow:     maxStreamReceiveWindow,
		MaxConnectionReceiveWindow: maxConnectionReceiveWindow,
		EnableDatagrams:            enableDatagrams,
		DisablePathMTUDiscovery:    disablePathMTUDiscovery,
		Allow0RTT:                  !disable0RTT,
	}
}
