package http

import (
	"context"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"strconv"
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

	server := &nethttp.Server{Handler: nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		_, _ = io.WriteString(w, "kcp")
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
		Transfer:       sts.TransferKCPHTTP,
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
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "kcp" {
		t.Fatalf("unexpected response body: %q", body)
	}
	if client.kcpSessions == nil {
		t.Fatal("KCP session metrics registry was not configured")
	}
	metrics := client.kcpSessions.snapshot()
	if metrics.Active != 1 {
		t.Fatalf("unexpected active KCP session count: %d", metrics.Active)
	}

	if err = server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-serveDone; err != nethttp.ErrServerClosed {
		t.Fatalf("unexpected server result: %v", err)
	}
}
