package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/logging"
	"github.com/ARM-DOE/sts/pathutils"
)

func main() {
	// Initialize command line arguments
	help := flag.Bool("help", false, "Print the help message")
	debug := flag.Bool("debug", false, "Log program flow")
	mode := flag.String("mode", "", "Mode: 'send' or 'receive'")
	confName := flag.String("conf", "", "Configuration file path")

	// Parse command line
	flag.Parse()

	// Validate
	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Parse configuration and initialize directories
	if *confName == "" {
		*confName = pathutils.Join(pathutils.GetRoot(), "conf", "dist."+*mode+".yaml")
	}
	config, err := NewConf(*confName)
	if err != nil {
		logging.Error("Failed to parse configuration:", *confName, err.Error())
		os.Exit(1)
	}

	// Useful for inspecting nested structs...
	// spew.Dump(config)

	// Run
	if *mode == "receive" || *mode == "" {
		if config.Receive == nil || config.Receive.Dirs == nil || config.Receive.Server == nil {
			panic("Missing required RECEIVER configuration")
		}
		logging.Init([]string{logging.Receive, logging.Msg}, initPath(config.Receive.Dirs.Logs, true), *debug)
		startReceiver(config.Receive)
		time.Sleep(1 * time.Second) // Give it time to start the server.
	}
	if *mode == "send" || *mode == "" {
		if config.Send == nil || config.Send.Sources == nil || len(config.Send.Sources) < 1 {
			panic("Missing required SENDER configuration")
		}
		logging.Init([]string{logging.Send, logging.Msg}, initPath(config.Send.Dirs.Logs, true), *debug)
		for _, source := range config.Send.Sources {
			startSender(source, config.Send.Dirs)
		}
	}

	// TODO: Add mechanism for graceful shutdown by sending "empty" data down the input channels
	// signaling to channel listeners to stop listening.  See documentation on Go pipelines.
	for {
		// This is a daemon, so keep on running.
		time.Sleep(5 * time.Second)
	}
}

func startReceiver(conf *ReceiveConf) {
	recvConf := &ReceiverConf{}
	recvConf.StageDir = initPath(conf.Dirs.Stage, true)
	recvConf.FinalDir = initPath(conf.Dirs.Final, true)
	recvConf.Port = conf.Server.Port
	if conf.Server.TLS {
		recvConf.TLSCert = conf.Server.TLSCert
		recvConf.TLSKey = conf.Server.TLSKey
	}

	scanChan := make(chan []ScanFile, 1)

	scanner := NewScanner(recvConf.StageDir, initPath(conf.Dirs.Cache, true), time.Second*1, 0, false, 1)
	finalizer := NewFinalizer(recvConf)
	receiver := NewReceiver(recvConf, finalizer)

	scanner.AddIgnore(fmt.Sprintf(`\%s$`, fileutils.LockExt))
	scanner.AddIgnore(fmt.Sprintf(`\%s$`, PartExt))
	scanner.AddIgnore(fmt.Sprintf(`\%s$`, CompExt))

	go scanner.Start(scanChan, nil)
	go finalizer.Start(scanChan)
	go receiver.Serve()

	logging.Debug(fmt.Sprintf("RECEIVER Ready: Port %d", recvConf.Port))
}

func startSender(source *SendSource, dirs *SendDirs) {
	outDir := initPath(pathutils.Join(dirs.Out, source.Target.Name), true)
	cacheDir := initPath(dirs.Cache, true)

	sendConf := &SenderConf{}
	sendConf.Compress = source.Compress
	sendConf.SourceName = source.Name
	sendConf.TargetName = source.Target.Name
	sendConf.TargetHost = source.Target.Host
	sendConf.TLSCert = source.Target.TLSCert
	sendConf.TLSKey = source.Target.TLSKey
	sendConf.BinSize = source.BinSize
	sendConf.Timeout = source.Timeout
	sendConf.MaxRetries = source.MaxRetries
	sendConf.PollInterval = source.PollInterval
	sendConf.PollDelay = source.PollDelay
	if sendConf.SourceName == "" {
		panic("Source name missing from configuration")
	}
	if sendConf.TargetName == "" {
		panic("Target name missing from configuration")
	}
	if sendConf.TargetHost == "" {
		panic("Target host missing from configuration")
	}
	if source.Target.TLS && (sendConf.TLSCert == "" || sendConf.TLSKey == "") {
		panic("TLS enabled but missing either TLSCert or TLSKey")
	}
	if sendConf.BinSize == 0 {
		sendConf.BinSize = 10 * 1024 * 1024
	}
	if sendConf.Timeout == 0 {
		sendConf.Timeout = 3600 * 60
	}
	if sendConf.MaxRetries == 0 {
		sendConf.MaxRetries = 10
	}
	if sendConf.PollInterval == 0 {
		sendConf.PollInterval = 60
	}
	if sendConf.PollDelay == 0 {
		sendConf.PollDelay = 5
	}

	// spew.Dump(sendConf)
	// spew.Dump(dirs)

	scanChan := make(chan []ScanFile, 1) // One batch at a time.
	sortChan := make(chan SortFile, source.Threads*2)
	sendChan := make(chan SendFile, source.Threads*2)
	loopChan := make(chan SendFile, source.Threads*2)
	pollChan := []chan SendFile{
		make(chan SendFile, source.Threads*2),
	}
	doneChan := []chan DoneFile{
		make(chan DoneFile, source.Threads*2),
		make(chan DoneFile, source.Threads*2),
	}

	scanner := NewScanner(outDir, cacheDir, time.Second*5, source.MinAge, true, 0)
	sorter := NewSorter(source.Tags)

	scanner.AddIgnore(fmt.Sprintf(`\%s$`, fileutils.LockExt))

	for i := 0; i < source.Threads; i++ {
		sender := NewSender(sendConf)
		go sender.StartPrep(sortChan, sendChan)
		go sender.StartSend(sendChan, loopChan, pollChan[:len(pollChan)])
	}

	poller := NewPoller(sendConf)

	go poller.Start(pollChan[0], sendChan, doneChan[:len(doneChan)])
	go sorter.Start(scanChan, sortChan, doneChan[0])
	go scanner.Start(scanChan, doneChan[1])

	logging.Debug(fmt.Sprintf("SENDER Ready: %s -> %s", sendConf.SourceName, sendConf.TargetName))
}

func initPath(path string, isdir bool) string {
	var err error
	if !filepath.IsAbs(path) {
		root := pathutils.GetRoot()
		path, err = filepath.Abs(pathutils.Join(root, path))
		if err != nil {
			logging.Error("Failed to initialize: ", path, err.Error())
		}
	}
	pdir := path
	if !isdir {
		pdir = filepath.Dir(pdir)
	}
	_, err = os.Stat(pdir)
	if os.IsNotExist(err) {
		logging.Debug("CONFIG Make Path:", pdir)
		os.MkdirAll(pdir, os.ModePerm)
	}
	return path
}

func initPathElements(i interface{}) {
	v := reflect.ValueOf(i)
	if v.IsNil() {
		return
	}
	t := reflect.TypeOf(i).Elem()
	n := t.NumField()
	for i := 0; i < n; i++ {
		index := []int{i}
		attName := t.FieldByIndex(index)
		attValue := reflect.Indirect(v).FieldByName(attName.Name)
		attValue.SetString(initPath(attValue.String(), true))
	}
}
