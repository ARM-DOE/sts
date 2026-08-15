package http

import (
	"errors"
	"testing"
)

func TestParseProtocol(t *testing.T) {
	tests := []struct {
		input string
		want  TransportProtocol
	}{
		{input: "", want: ProtocolAuto},
		{input: "auto", want: ProtocolAuto},
		{input: "AUTO", want: ProtocolAuto},
		{input: "http", want: ProtocolHTTP1},
		{input: "http1", want: ProtocolHTTP1},
		{input: "http1.1", want: ProtocolHTTP1},
		{input: "https", want: ProtocolHTTPS},
		{input: "h2", want: ProtocolHTTPS},
		{input: "http2", want: ProtocolHTTPS},
		{input: "http3", want: ProtocolHTTP3},
		{input: "h3", want: ProtocolHTTP3},
		{input: "QUIC", want: ProtocolHTTP3},
		{input: "kcp-http", want: ProtocolKCPHTTP},
		{input: "unknown", want: ProtocolHTTPS},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			if got := ParseProtocol(test.input); got != test.want {
				t.Fatalf("ParseProtocol(%q) = %s, want %s", test.input, got, test.want)
			}
		})
	}
}

func TestTransportProtocolString(t *testing.T) {
	tests := []struct {
		protocol TransportProtocol
		want     string
	}{
		{protocol: ProtocolAuto, want: "auto"},
		{protocol: ProtocolHTTP1, want: "http"},
		{protocol: ProtocolHTTPS, want: "https"},
		{protocol: ProtocolHTTP3, want: "http3"},
		{protocol: ProtocolKCPHTTP, want: "kcp-http"},
		{protocol: TransportProtocol(99), want: "unknown"},
	}

	for _, test := range tests {
		if got := test.protocol.String(); got != test.want {
			t.Errorf("TransportProtocol(%d).String() = %q, want %q", test.protocol, got, test.want)
		}
	}
}

func TestIsHTTP3SpecificError(t *testing.T) {
	if isHTTP3SpecificError(nil) {
		t.Fatal("isHTTP3SpecificError(nil) = true, want false")
	}

	markers := []string{
		"QUIC handshake failed",
		"application_error 42",
		"timeout: no recent network activity",
		"protocol_violation",
		"UDP socket failed",
		"datagram too large",
		"handshake did not complete",
	}
	for _, message := range markers {
		t.Run(message, func(t *testing.T) {
			if !isHTTP3SpecificError(errors.New(message)) {
				t.Fatalf("isHTTP3SpecificError(%q) = false, want true", message)
			}
		})
	}

	if isHTTP3SpecificError(errors.New("certificate is expired")) {
		t.Fatal("generic TLS error classified as HTTP/3-specific")
	}
}
