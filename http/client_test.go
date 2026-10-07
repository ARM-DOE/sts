package http

import (
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// func startServer() {
// 	server := &Server{
// 		Host: "localhost",
// 		Port: 1992,
// 	}
// 	stop := make(chan bool)
// 	done := make(chan bool)
// 	go server.Serve(stop, done)
// }

func TestSend(t *testing.T) {

}

func TestClientDestroyCancelsActiveRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		close(requestStarted)
		<-releaseHandler
	}))
	defer server.Close()
	defer close(releaseHandler)

	client := &Client{
		root:    server.URL,
		Timeout: time.Hour,
	}
	requestDone := make(chan error, 1)
	go func() {
		_, err := client.Recover()
		requestDone <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}

	client.Destroy()

	select {
	case err := <-requestDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request was not canceled")
	}
}

func TestValidateRequestIsReplayableAndIdempotent(t *testing.T) {
	var request *stdhttp.Request
	transport := roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
		request = req
		return &stdhttp.Response{
			StatusCode: stdhttp.StatusOK,
			Header:     make(stdhttp.Header),
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})
	client := &Client{
		root: "https://example.test",
		ctx:  context.Background(),
		client: &BandwidthLoggingClient{
			Client:  &stdhttp.Client{Transport: transport},
			monitor: &bandwidthMonitorTransport{transport: transport},
		},
	}

	if _, err := client.Validate(nil); err != nil {
		t.Fatal(err)
	}
	if request.GetBody == nil {
		t.Fatal("validation body is not replayable")
	}
	if request.Header.Get("Idempotency-Key") == "" {
		t.Fatal("validation request is not marked idempotent")
	}
}
