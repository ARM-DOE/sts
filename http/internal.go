package http

import (
	"fmt"
	"net/http"

	"code.arm.gov/dataflow/sts/log"
)

// Internal responds to internal HTTP command requests for altering the behavior
// of the running executable
type Internal struct {
	Port int
}

// Serve starts HTTP server.
func (s *Internal) Serve(stop <-chan bool, done chan<- bool) {
	http.Handle("/debug", http.HandlerFunc(s.routeDebug))

	addr := fmt.Sprintf("localhost:%d", s.Port)
	server := NewGracefulServer(
		&http.Server{Addr: addr, Handler: http.DefaultServeMux}, nil)
	go func(done chan<- bool, svr *GracefulServer) {
		err := svr.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Error(err.Error())
		}
		done <- true
	}(done, server)
	<-stop
	server.Close()
}

func (s *Internal) routeDebug(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		fallthrough
	case http.MethodPost:
		log.SetDebug(!log.GetDebug())
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}
