package http

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestNewHTTP3ServerEnablesSessionTickets(t *testing.T) {
	input := &tls.Config{
		SessionTicketsDisabled: true,
		NextProtos:             []string{"h2"},
	}

	s := NewHTTP3Server("localhost:2026", http.NewServeMux(), input, nil)
	if s == nil || s.tlsConfig == nil {
		t.Fatal("expected HTTP3 server with TLS config")
	}

	if s.tlsConfig.SessionTicketsDisabled {
		t.Fatal("expected HTTP3 TLS config to enable session tickets")
	}

	if input.SessionTicketsDisabled != true {
		t.Fatal("expected input TLS config to remain unchanged")
	}

	hasH3 := false
	for _, proto := range s.tlsConfig.NextProtos {
		if proto == "h3" {
			hasH3 = true
			break
		}
	}
	if !hasH3 {
		t.Fatal("expected HTTP3 ALPN to include h3")
	}
}
