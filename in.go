package main

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
)

// AppIn is the struct container for the incoming portion of the STS app.
type AppIn struct {
	root      string
	rawConf   *InConf
	conf      *ReceiverConf
	scanChan  chan []ScanFile
	scanner   *Scanner
	finalizer *Finalizer
	server    *Receiver
}

func (a *AppIn) initConf() {
	a.conf = &ReceiverConf{}
	a.conf.Keys = a.rawConf.Keys
	a.conf.Sources = a.rawConf.Sources
	stage := a.rawConf.Dirs.Stage
	if stage == "" {
		stage = "stage"
	}
	final := a.rawConf.Dirs.Final
	if final == "" {
		final = "final"
	}
	a.conf.StageDir = InitPath(a.root, stage, true)
	a.conf.FinalDir = InitPath(a.root, final, true)
	a.conf.Port = a.rawConf.Server.Port
	if a.conf.Port == 0 {
		panic("Server port not specified")
	}
	a.conf.Compression = a.rawConf.Server.Compression
	if a.rawConf.Server.TLSCertPath != "" && a.rawConf.Server.TLSKeyPath != "" {
		certPath := InitPath(a.root, a.rawConf.Server.TLSCertPath, false)
		keyPath := InitPath(a.root, a.rawConf.Server.TLSKeyPath, false)
		var err error
		if a.conf.TLS, err = httputils.GetTLSConf(certPath, keyPath, ""); err != nil {
			panic(err)
		}
	}
}

// Start initializes and starts the receiver.
func (a *AppIn) Start(stop <-chan bool) <-chan bool {
	a.initConf()

	// TODO: Use the scanner to just scan once at startup and then put those files on the "finalize" channel.
	// Setup the HTTP server to send on what is now the "scan" channel instead of relying on the scanner
	// and file extensions.

	a.scanChan = make(chan []ScanFile)

	scanConf := ScannerConf{
		ScanDir: a.conf.StageDir,
		MaxAge:  time.Hour * 1,
		Delay:   time.Second * 10,
		Nested:  1,
	}

	a.finalizer = NewFinalizer(a.conf)
	a.scanner = NewScanner(&scanConf)
	a.server = NewReceiver(a.conf, a.finalizer)

	// TODO: on startup, receiver should check for .cmp files with .part counterparts where the .cmp
	// indicates a completed file.  This is possible because the companion is written before the counterpart
	// is renamed.  I could flip that but then there could be a race condition on reading a companion file
	// that hasn't been updated yet (finalize).

	// OK to ignore returned error because these are valid constants and will compile.
	scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(fileutils.LockExt)))
	scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(PartExt)))
	scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(CompExt)))

	var wg sync.WaitGroup
	wg.Add(3)

	scanStop := make(chan bool)
	httpStop := make(chan bool)

	go func(a *AppIn, wg *sync.WaitGroup) {
		defer wg.Done()
		a.finalizer.Start(a.scanChan)
		logging.Debug("IN Finalize Done")
	}(a, &wg)
	go func(a *AppIn, wg *sync.WaitGroup, stop <-chan bool) {
		defer wg.Done()
		a.scanner.Start(a.scanChan, nil, stop)
		logging.Debug("IN Scanner Done")
	}(a, &wg, scanStop)
	go func(a *AppIn, wg *sync.WaitGroup, stop <-chan bool) {
		defer wg.Done()
		a.server.Serve(stop)
		logging.Debug("IN Server Done")
	}(a, &wg, httpStop)

	logging.Debug(fmt.Sprintf("RECEIVER Ready: Port %d", a.conf.Port))

	done := make(chan bool)
	go func(stop <-chan bool, scanStop chan<- bool, done chan<- bool) {
		<-stop
		logging.Debug("IN Got Shutdown Signal")
		close(scanStop)
		close(httpStop)
		wg.Wait()
		close(done)
	}(stop, scanStop, done)
	return done
}
