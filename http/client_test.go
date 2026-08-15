package http

import (
	"context"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
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
