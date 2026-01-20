package http

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/arm-doe/sts/log"
	"github.com/quic-go/quic-go"
)

// TransportProtocol represents the underlying protocol
type TransportProtocol int

const (
	// ProtocolAuto tries HTTP3 first, falls back to HTTPS
	ProtocolAuto TransportProtocol = iota
	// ProtocolHTTP1 forces HTTP/1.1 without TLS
	ProtocolHTTP1
	// ProtocolHTTPS forces HTTPS (HTTP/1.1 or HTTP/2 with TLS)
	ProtocolHTTPS
	// ProtocolHTTP3 forces HTTP/3 over QUIC
	ProtocolHTTP3
)

// ParseProtocol converts a string to TransportProtocol
func ParseProtocol(s string) TransportProtocol {
	switch strings.ToLower(s) {
	case "http3", "h3", "quic":
		return ProtocolHTTP3
	case "https", "h2", "http2":
		return ProtocolHTTPS
	case "http", "http1", "http1.1":
		return ProtocolHTTP1
	case "auto", "":
		return ProtocolAuto
	default:
		return ProtocolHTTPS
	}
}

// String returns the string representation of the protocol
func (p TransportProtocol) String() string {
	switch p {
	case ProtocolHTTP3:
		return "http3"
	case ProtocolHTTPS:
		return "https"
	case ProtocolHTTP1:
		return "http"
	case ProtocolAuto:
		return "auto"
	default:
		return "unknown"
	}
}

// TransportBuilder creates appropriate transport based on protocol
type TransportBuilder struct {
	TLSConfig   *tls.Config
	Protocol    TransportProtocol
	LogInterval time.Duration
	LogPrefix   string
	// QUIC configuration (for HTTP3)
	QUICConfig *quic.Config
}

// Build creates a BandwidthLoggingClient with the appropriate protocol
func (tb *TransportBuilder) Build() (*BandwidthLoggingClient, error) {
	switch tb.Protocol {
	case ProtocolHTTP3:
		return tb.buildHTTP3Client()
	case ProtocolHTTPS, ProtocolHTTP1:
		return tb.buildHTTPSClient()
	case ProtocolAuto:
		return tb.buildAutoClient()
	default:
		return tb.buildHTTPSClient()
	}
}

// buildHTTPSClient creates a traditional HTTPS/HTTP client
func (tb *TransportBuilder) buildHTTPSClient() (*BandwidthLoggingClient, error) {
	// Clone the default transport
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tb.TLSConfig != nil {
		tr.TLSClientConfig = tb.TLSConfig
	}
	tr.DisableKeepAlives = true

	monitor := &bandwidthMonitorTransport{transport: tr}
	client := newBandwidthLoggingClient(monitor, tb.LogInterval, func(i ...interface{}) {
		log.Info(append([]interface{}{tb.LogPrefix}, i...)...)
	})

	return client, nil
}

// buildAutoClient creates a client that tries HTTP3 first with fallback
func (tb *TransportBuilder) buildAutoClient() (*BandwidthLoggingClient, error) {
	// Try HTTP3 first
	http3Client, err := tb.buildHTTP3Client()
	if err != nil {
		log.Debug("HTTP3 initialization failed, falling back to HTTPS:", err)
		return tb.buildHTTPSClient()
	}

	// Set up fallback function
	http3Client.FallbackOnError = func(originalErr error) (*BandwidthLoggingClient, error) {
		log.Info(fmt.Sprintf("%sHTTP3 connection failed (%v), falling back to HTTPS", tb.LogPrefix, originalErr))

		// Build HTTPS client
		httpsBuilder := &TransportBuilder{
			TLSConfig:   tb.TLSConfig,
			Protocol:    ProtocolHTTPS,
			LogInterval: tb.LogInterval,
			LogPrefix:   tb.LogPrefix,
		}
		return httpsBuilder.buildHTTPSClient()
	}

	return http3Client, nil
}

// isHTTP3SpecificError checks if an error should trigger fallback from HTTP3 to HTTPS
func isHTTP3SpecificError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Check for QUIC-specific errors
	quicErrors := []string{
		"quic",
		"application_error",
		"timeout: no recent network activity",
		"no_error",
		"protocol_violation",
		"internal_error",
		"connection_refused",
		"udp",
		"datagram too large",
		"handshake did not complete",
	}

	for _, marker := range quicErrors {
		if strings.Contains(errStr, marker) {
			return true
		}
	}

	return false
}
