package sts

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/ARM-DOE/sts/httputils"
	"github.com/ARM-DOE/sts/logging"
)

// AppIn is the struct container for the incoming portion of the STS app.
type AppIn struct {
	Root      string
	RawConf   *InConf
	conf      *ReceiverConf
	finChan   chan []ScanFile
	finalizer *Finalizer
	server    *Receiver
}

func (a *AppIn) initConf() {
	a.conf = &ReceiverConf{}
	a.conf.Keys = a.RawConf.Keys
	a.conf.Sources = a.RawConf.Sources
	stage := a.RawConf.Dirs.Stage
	if stage == "" {
		stage = "stage"
	}
	final := a.RawConf.Dirs.Final
	if final == "" {
		final = "final"
	}
	a.conf.StageDir = InitPath(a.Root, stage, true)
	a.conf.FinalDir = InitPath(a.Root, final, true)
	a.conf.Port = a.RawConf.Server.Port
	if a.conf.Port == 0 {
		panic("Server port not specified")
	}
	a.conf.Compression = a.RawConf.Server.Compression
	if a.RawConf.Server.TLSCertPath != "" && a.RawConf.Server.TLSKeyPath != "" {
		certPath := InitPath(a.Root, a.RawConf.Server.TLSCertPath, false)
		keyPath := InitPath(a.Root, a.RawConf.Server.TLSKeyPath, false)
		var err error
		if a.conf.TLS, err = httputils.GetTLSConf(certPath, keyPath, ""); err != nil {
			panic(err)
		}
	}
}

func (a *AppIn) recover() {
	scanConf := ScannerConf{
		ScanDir: a.conf.StageDir,
		Nested:  1,
	}
	// Look for any files left behind after previous run.
	scanner, _ := NewScanner(&scanConf) // Ignore error because we don't have a cache to read.
	files, _ := scanner.Scan()
	if len(files) == 0 {
		return
	}
	var out []ScanFile
	var rescan bool
	for _, f := range files {
		ext := filepath.Ext(f.GetPath(false))
		if ext == PartExt {
			continue
		}
		if ext == CompExt {
			logging.Debug("IN Reading Companion:", f.GetRelPath())
			base := strings.TrimSuffix(f.GetPath(false), CompExt)
			comp, err := ReadCompanion(base)
			if err != nil {
				logging.Error("Failed to read companion:", f.GetPath(false), err.Error())
				continue
			}
			if _, err := os.Stat(base + PartExt); !os.IsNotExist(err) {
				if comp.IsComplete() {
					if err := os.Rename(base+PartExt, base); err != nil {
						logging.Error("Failed to drop \"partial\" extension:", err.Error())
						continue
					}
					rescan = true
				}
			} else if _, err := os.Stat(base); os.IsNotExist(err) {
				logging.Debug("IN Removing Orphaned Companion:", f.GetPath(false))
				if err = comp.Delete(); err != nil {
					logging.Error("Failed to remove companion file:", f.GetPath(false), err.Error())
					continue
				}
			}
			continue
		}
		logging.Debug("IN Found Stage File:", f.GetRelPath())
		out = append(out, f)
	}
	if rescan {
		scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(CompExt)))
		scanConf.AddIgnoreString(fmt.Sprintf(`%s$`, regexp.QuoteMeta(PartExt)))
		scanner.Reset()
		files, _ = scanner.Scan()
		for _, f := range files {
			found := false
			for _, fo := range out {
				if fo.GetPath(false) == f.GetPath(false) {
					found = true
					break
				}
			}
			if found {
				continue
			}
			logging.Debug("IN Found Stage File:", f.GetRelPath())
			out = append(out, f)
		}
	}
	if len(out) > 0 {
		a.finChan <- out
	}
}

// Start initializes and starts the receiver.
func (a *AppIn) Start(stop <-chan bool) <-chan bool {
	a.initConf()

	a.finChan = make(chan []ScanFile, 10)

	a.recover()

	a.finalizer = NewFinalizer(a.conf)
	a.server = NewReceiver(a.conf, a.finalizer)

	var wg sync.WaitGroup
	wg.Add(2)

	httpStop := make(chan bool)

	go func(a *AppIn, wg *sync.WaitGroup) {
		defer wg.Done()
		a.finalizer.Start(a.finChan)
		logging.Debug("IN Finalize Done")
	}(a, &wg)
	go func(a *AppIn, wg *sync.WaitGroup, stop <-chan bool) {
		defer wg.Done()
		a.server.Serve(a.finChan, stop)
		close(a.finChan)
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
