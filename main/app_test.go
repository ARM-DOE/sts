package main

import (
	"context"
	"errors"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	stshttp "github.com/arm-doe/sts/http"
	"github.com/arm-doe/sts/log"
)

func TestStopClientsCancelsRequestsBeforeWaiting(t *testing.T) {
	log.InitExternal(newLogger(false))
	requestStarted := make(chan struct{})
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		close(requestStarted)
		<-r.Context().Done()
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, portString, err := net.SplitHostPort(serverURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}

	httpClient := &stshttp.Client{
		TargetHost: host,
		TargetPort: port,
		Timeout:    time.Hour,
	}
	requestDone := make(chan error, 1)
	go func() {
		_, err := httpClient.Recover()
		requestDone <- err
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}

	stop := make(chan bool)
	done := make(chan bool)
	go func() {
		<-stop
		err := <-requestDone
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected request cancellation, got %v", err)
		}
		done <- true
	}()

	client := &clientApp{httpClient: httpClient}
	app := &app{
		clients: []*clientApp{client},
		clientStop: map[chan<- bool]<-chan bool{
			stop: done,
		},
	}
	stopped := make(chan struct{})
	go func() {
		app.stopClients(false)
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("forced client stop waited on an active request")
	}
}
