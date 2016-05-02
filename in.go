package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
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
	a.conf.StageDir = InitPath(a.root, a.rawConf.Dirs.Stage, true)
	a.conf.FinalDir = InitPath(a.root, a.rawConf.Dirs.Final, true)
	a.conf.Port = a.rawConf.Server.Port
	a.conf.Compression = a.rawConf.Server.Compression
	if a.rawConf.Server.TLS {
		a.conf.TLSCert = a.rawConf.Server.TLSCert
		a.conf.TLSKey = a.rawConf.Server.TLSKey
		for _, p := range []*string{&a.conf.TLSCert, &a.conf.TLSKey} {
			if *p == "" {
				panic("TLS enabled but missing TLS Cert and/or TLS Key")
			}
			if *p != "" && !filepath.IsAbs(*p) {
				*p = filepath.Join(a.root, *p)
			}
			if _, err := os.Stat(*p); os.IsNotExist(err) {
				panic("TLS enabled but cannot find TLS Cert and/or TLS Key")
			}
		}
	}
}

// Start initializes and starts the receiver.
func (a *AppIn) Start(stop <-chan bool) <-chan bool {
	a.initConf()

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

	scanConf.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(fileutils.LockExt)))
	scanConf.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(PartExt)))
	scanConf.AddIgnore(fmt.Sprintf(`%s$`, regexp.QuoteMeta(CompExt)))

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
