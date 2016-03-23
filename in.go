package main

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/logging"
)

// AppIn is the struct container for the incoming portion of the STS app.
type AppIn struct {
	root      string
	rawConf   *ReceiveConf
	conf      *ReceiverConf
	scanChan  chan []ScanFile
	scanner   *Scanner
	finalizer *Finalizer
	server    *Receiver
}

func (a *AppIn) initConf() {
	a.conf = &ReceiverConf{}
	a.conf.StageDir = InitPath(a.root, a.rawConf.Dirs.Stage, true)
	a.conf.FinalDir = InitPath(a.root, a.rawConf.Dirs.Final, true)
	a.conf.Port = a.rawConf.Server.Port
	if a.rawConf.Server.TLS {
		a.conf.TLSCert = a.rawConf.Server.TLSCert
		a.conf.TLSKey = a.rawConf.Server.TLSKey
	}
}

// Start initializes and starts the receiver.
func (a *AppIn) Start(stop <-chan bool) <-chan bool {
	a.initConf()

	a.scanChan = make(chan []ScanFile)

	a.finalizer = NewFinalizer(a.conf)
	a.scanner = NewScanner(a.conf.StageDir, InitPath(a.root, a.rawConf.Dirs.Cache, true), time.Second*1, 0, false, 1)
	a.server = NewReceiver(a.conf, a.finalizer)

	// TODO: on startup, receiver should check for .cmp files with .part counterparts where the .cmp
	// indicates a completed file.  This is possible because the companion is written before the counterpart
	// is renamed.  I could flip that but then there could be a race condition on reading a companion file
	// that hasn't been updated yet (finalize).

	a.scanner.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(fileutils.LockExt)))
	a.scanner.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(PartExt)))
	a.scanner.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(CompExt)))

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
