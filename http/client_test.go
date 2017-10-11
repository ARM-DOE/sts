package http

import "testing"

func startServer() {
	server := &Server{
		Host: "localhost",
		Port: 1992,
	}
	stop := make(chan bool)
	done := make(chan bool)
	go server.Serve(stop, done)
}

func TestSend(t *testing.T) {

}
