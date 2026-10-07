package http

import (
	"context"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arm-doe/sts"
)

func testKCPConf() *sts.KCPConf {
	return &sts.KCPConf{
		Tuning:            sts.KCPTuningBalanced,
		MTU:               1350,
		SendWindow:        1024,
		ReceiveWindow:     1024,
		DataShards:        10,
		ParityShards:      2,
		NoDelay:           true,
		Interval:          20 * time.Millisecond,
		FastResend:        2,
		ACKNoDelay:        true,
		SocketReadBuffer:  4 * 1024 * 1024,
		SocketWriteBuffer: 4 * 1024 * 1024,
		IdleTimeout:       time.Minute,
	}
}

func TestKCPHTTPTransport(t *testing.T) {
	certificateServer := httptest.NewTLSServer(nethttp.HandlerFunc(func(nethttp.ResponseWriter, *nethttp.Request) {}))
	serverTLS := certificateServer.TLS.Clone()
	clientTLS := certificateServer.Client().Transport.(*nethttp.Transport).TLSClientConfig.Clone()
	certificateServer.Close()

	serverConf := testKCPConf()
	listener, err := ListenKCP("127.0.0.1:0", serverConf)
	if err != nil {
		t.Fatal(err)
	}

	releaseResponse := make(chan struct{})
	server := &nethttp.Server{Handler: nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		_, _ = io.WriteString(w, "kcp")
		w.(nethttp.Flusher).Flush()
		<-releaseResponse
	})}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(tlsKCPListener(listener, serverTLS))
	}()

	host, portString, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	clientConf := testKCPConf()
	clientConf.Host = host
	clientConf.Port = port
	builder := &TransportBuilder{
		Protocol:       ProtocolKCPHTTP,
		KCPConfig:      clientConf,
		MaxConnections: 1,
		TLSConfig:      clientTLS,
	}
	client, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	client.Timeout = 5 * time.Second
	defer client.Stop()

	response, err := client.Get("https://" + listener.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	if client.kcpSessions == nil {
		t.Fatal("KCP session metrics registry was not configured")
	}
	metrics := client.kcpSessions.snapshot()
	if metrics.Active != 1 {
		t.Fatalf("unexpected active KCP session count: %d", metrics.Active)
	}
	close(releaseResponse)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "kcp" {
		t.Fatalf("unexpected response body: %q", body)
	}
	if err = server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-serveDone; err != nethttp.ErrServerClosed {
		t.Fatalf("unexpected server result: %v", err)
	}
}

func TestParseKCPHTTPProtocol(t *testing.T) {
	if protocol := ParseProtocol("kcp-http"); protocol != ProtocolKCPHTTP {
		t.Fatalf("ParseProtocol(%q) = %s", "kcp-http", protocol)
	}
	if ProtocolKCPHTTP.String() != "kcp-http" {
		t.Fatalf("unexpected canonical KCP protocol: %s", ProtocolKCPHTTP)
	}
}

func TestKCPClientIdleTimeoutPrecedesServer(t *testing.T) {
	timeout := 2 * time.Minute
	if got := kcpClientIdleTimeout(timeout); got != time.Minute {
		t.Fatalf("unexpected client idle timeout: %s", got)
	}
}

func TestKCPResponseHeaderTimeout(t *testing.T) {
	if kcpResponseHeaderTimeout != 30*time.Second {
		t.Fatalf("unexpected KCP response header timeout: %s", kcpResponseHeaderTimeout)
	}
}

func TestKCPCounterDeltaHandlesReset(t *testing.T) {
	if got := kcpCounterDelta(15, 10); got != 5 {
		t.Fatalf("unexpected counter delta: %d", got)
	}
	if got := kcpCounterDelta(3, 10); got != 3 {
		t.Fatalf("unexpected reset counter delta: %d", got)
	}
}

func TestKCPActiveUploadSurvivesIdleTimeout(t *testing.T) {
	serverConf := testKCPConf()
	serverConf.IdleTimeout = 50 * time.Millisecond
	listener, err := ListenKCP("127.0.0.1:0", serverConf)
	if err != nil {
		t.Fatal(err)
	}
	server := &nethttp.Server{
		IdleTimeout: serverConf.IdleTimeout,
		Handler: nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
		}),
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	host, portString, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	clientConf := testKCPConf()
	clientConf.Host = host
	clientConf.Port = port
	clientConf.IdleTimeout = serverConf.IdleTimeout
	client, err := (&TransportBuilder{
		Protocol:       ProtocolKCPHTTP,
		KCPConfig:      clientConf,
		MaxConnections: 1,
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	reader, writer := io.Pipe()
	writeDone := make(chan error, 1)
	go func() {
		defer writer.Close()
		for range 10 {
			if _, err := io.Copy(writer, strings.NewReader("payload")); err != nil {
				writeDone <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		writeDone <- nil
	}()
	request, err := nethttp.NewRequest(nethttp.MethodPut, "http://"+listener.Addr().String()+"/", reader)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if err = <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err = server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-serveDone; err != nethttp.ErrServerClosed {
		t.Fatalf("unexpected server result: %v", err)
	}
}
