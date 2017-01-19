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

	a.scanChan = make(chan []ScanFile, 10)

	scanConf := ScannerConf{
		ScanDir: a.conf.StageDir,
		MaxAge:  time.Hour * 1,
		Nested:  1,
	}

	// TODO: use the scanner to look for orphaned companions...

	// OK to ignore returned error because these are valid constants and will compile.
	scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(fileutils.LockExt)))
	scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(PartExt)))
	scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(CompExt)))

	// Scan once for files not finalized before previous shutdown.
	a.scanner, _ = NewScanner(&scanConf) // Ignore error because we don't have a cache to read.
	files, _ := a.scanner.Scan()
	if len(files) > 0 {
		for _, f := range files {
			logging.Debug("IN Found Stage File:", f.GetRelPath())
		}
		a.scanChan <- files
	}

	a.finalizer = NewFinalizer(a.conf)
	a.server = NewReceiver(a.conf, a.finalizer)

	var wg sync.WaitGroup
	wg.Add(2)

	httpStop := make(chan bool)

	go func(a *AppIn, wg *sync.WaitGroup) {
		defer wg.Done()
		a.finalizer.Start(a.scanChan)
		logging.Debug("IN Finalize Done")
	}(a, &wg)
	go func(a *AppIn, wg *sync.WaitGroup, stop <-chan bool) {
		defer wg.Done()
		a.server.Serve(a.scanChan, stop)
		close(a.scanChan)
		logging.Debug("IN Server Done")
	}(a, &wg, httpStop)

	logging.Debug(fmt.Sprintf("RECEIVER Ready: Port %d", a.conf.Port))

	done := make(chan bool)
	go func(stop <-chan bool, done chan<- bool) {
		<-stop
		logging.Debug("IN Got Shutdown Signal")
		close(httpStop)
		wg.Wait()
		close(done)
	}(stop, done)
	return done
}
