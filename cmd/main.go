package main

import (
	"flag"
	"fmt"
	"sync"

	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/payload"
	"code.arm.gov/dataflow/sts/stage"
	// For profiling...
	// "net/http"
	// _ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
)

const modeSend = "out"
const modeRecv = "in"
const modeAuto = "auto"

var (
	// Version is set based on -X option passed at build.
	Version = ""
	// BuildTime is set based on -X option passed at build.
	BuildTime = ""
)

const (
	defLogs    = "logs"
	defLogMsgs = "messages"
	defLogOut  = "outgoing_to"
	defLogIn   = "incoming_from"
	defOut     = "outgoing_to"
	defCache   = ".sts"
	defStage   = "stage"
	defFinal   = "incoming_from"
)

func main() {
	app := newApp()
	app.run()
}

// getRoot returns the STS root.  It will use $STS_HOME and fall back to the
// directory of the executable.
func getRoot() (root string) {
	root = os.Getenv("STS_HOME")
	if root == "" {
		root, _ = filepath.Abs(filepath.Dir(os.Args[0]))
	}
	return
}

type app struct {
	debug      bool
	once       bool
	mode       string
	root       string
	confPath   string
	conf       *conf
	clients    []*clientApp
	serverStop chan<- bool
	serverDone <-chan bool
	clientStop map[chan<- bool]<-chan bool
}

func newApp() *app {
	var err error

	// Initialize command line arguments
	help := flag.Bool("help", false, "Print the help message")
	vers := flag.Bool("version", false, "Print version information")
	debug := flag.Bool("debug", false, "Log program flow")
	mode := flag.String("mode", modeAuto, fmt.Sprintf("Mode: \"%s\", \"%s\", \"%s\"", modeSend, modeRecv, modeAuto))
	loop := flag.Bool("loop", false, "Run in a loop, i.e. don't exit until interrupted")
	confPath := flag.String("conf", "", "Configuration file path")

	// Parse command line
	flag.Parse()

	// Validate
	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *vers {
		fmt.Printf("%s @ %s\n", Version, BuildTime)
		os.Exit(0)
	}

	a := &app{
		debug:    *debug,
		mode:     strings.ToLower(*mode),
		once:     !*loop,
		confPath: *confPath,
		root:     getRoot(),
	}

	// Parse configuration
	if a.confPath == "" {
		if a.mode == modeAuto {
			a.confPath = filepath.Join(getRoot(), "conf", "sts.yaml")
		} else {
			a.confPath = filepath.Join(getRoot(), "conf", "sts."+a.mode+".yaml")
		}
	} else if !filepath.IsAbs(a.confPath) {
		if a.confPath, err = filepath.Abs(a.confPath); err != nil {
			panic(err.Error())
		}
	}
	conf, err := newConf(a.confPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse configuration:\n%s", err.Error()))
	}
	a.conf = conf

	return a
}

func (a *app) run() {
	i := a.startServer()
	o := a.startClients()
	if !i && !o {
		panic("Not configured to do anything?")
	}
	// Run until we get a signal to shutdown if running ONLY as a "receiver" or
	// if configured to run as a sender in a loop.
	if !a.once || (i && !o) {
		// if !i {
		// 	// Start a profiler server only if running solely "out" mode in a
		// 	// loop.  We can use the "in" server otherwise.
		// 	// https://golang.org/pkg/net/http/pprof/
		// 	go func() {
		// 		http.ListenAndServe("localhost:6060", nil)
		// 	}()
		// }
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, os.Interrupt, os.Kill)

		log.Debug("Waiting for signal...")

		<-sc // Block until we get a signal.
	}
	a.stopClients()
	a.stopServer()
}

func (a *app) startServer() bool {
	if a.mode != modeRecv && a.mode != modeAuto {
		return false
	}
	if a.conf.Server == nil || a.conf.Server.Dirs == nil || a.conf.Server.Server == nil {
		if a.mode == modeAuto {
			return false
		}
		panic("Missing required SERVER configuration")
	}
	conf := a.conf.Server
	if conf.Server.Port == 0 {
		panic("HTTP port not specified")
	}
	dirs := conf.Dirs
	if dirs.Logs == "" {
		dirs.Logs = defLogs
	}
	if dirs.LogsMsg == "" {
		dirs.LogsMsg = defLogMsgs
	}
	if dirs.LogsIn == "" {
		dirs.LogsIn = defLogIn
	}
	if dirs.Stage == "" {
		dirs.Stage = defStage
	}
	if dirs.Final == "" {
		dirs.Final = defFinal
	}
	dirPaths := map[string]string{
		"log":   filepath.Join(dirs.Logs, dirs.LogsMsg),
		"logIn": filepath.Join(dirs.Logs, dirs.LogsIn),
		"stage": dirs.Stage,
		"final": dirs.Final,
		"serve": dirs.Serve,
	}
	var err error
	for key, path := range dirPaths {
		if path == "" {
			continue
		}
		if dirPaths[key], err = fileutil.InitPath(a.root, path, true); err != nil {
			panic(err)
		}
	}
	log.Init(dirPaths["log"], a.debug)
	stager := stage.New(
		dirPaths["stage"],
		dirPaths["final"],
		log.NewReceive(dirPaths["logIn"]))
	if err = stager.Recover(); err != nil {
		panic(err)
	}
	server := &http.Server{
		ServeDir:       dirPaths["serve"],
		Host:           conf.Server.Host,
		Port:           conf.Server.Port,
		Sources:        conf.Sources,
		Keys:           conf.Keys,
		Compression:    conf.Server.Compression,
		DecoderFactory: payload.NewDecoder,
		GateKeeper:     stager,
	}
	if conf.Server.TLSCertPath != "" && conf.Server.TLSKeyPath != "" {
		var certPath, keyPath string
		if certPath, err = fileutil.InitPath(
			a.root, conf.Server.TLSCertPath, false); err != nil {
			panic(err)
		}
		if keyPath, err = fileutil.InitPath(
			a.root, conf.Server.TLSKeyPath, false); err != nil {
			panic(err)
		}
		if server.TLS, err = http.GetTLSConf(certPath, keyPath, ""); err != nil {
			panic(err)
		}
	}
	stop := make(chan bool)
	done := make(chan bool)
	a.serverStop = stop
	a.serverDone = done
	go server.Serve(stop, done)
	return true
}

func (a *app) stopServer() {
	log.Debug("Stopping server...")
	if a.serverStop != nil {
		a.serverStop <- true
	}
	log.Debug("Waiting for server to exit...")
	if a.serverDone != nil {
		<-a.serverDone
	}
	log.Debug("Server stopped...")
}

func (a *app) startClients() bool {
	if a.mode != modeSend && a.mode != modeAuto {
		return false
	}
	if a.conf.Client == nil || a.conf.Client.Sources == nil || len(a.conf.Client.Sources) < 1 {
		if a.mode == modeAuto {
			return false
		}
		panic("Missing required CLIENT (aka SENDER) configuration")
	}
	var err error
	dirs := a.conf.Client.Dirs
	if dirs.Logs == "" {
		dirs.Logs = defLogs
	}
	if dirs.LogsMsg == "" {
		dirs.LogsMsg = defLogMsgs
	}
	if dirs.LogsOut == "" {
		dirs.LogsOut = defLogOut
	}
	if dirs.Cache == "" {
		dirs.Cache = defCache
	}
	if dirs.Out == "" {
		dirs.Out = defOut
	}
	logMsgPath, err := fileutil.InitPath(
		a.root, filepath.Join(dirs.Logs, dirs.LogsMsg), true)
	if err != nil {
		panic(err)
	}
	log.Init(logMsgPath, a.debug)
	logOutPath, err := fileutil.InitPath(
		a.root, filepath.Join(dirs.Logs, dirs.LogsOut), true)
	if err != nil {
		panic(err)
	}
	a.clientStop = make(map[chan<- bool]<-chan bool)
	watching := make(map[string]bool)
	for _, source := range a.conf.Client.Sources {
		c := &clientApp{
			root:         a.root,
			conf:         source,
			dirCache:     dirs.Cache,
			dirOut:       dirs.Out,
			dirOutFollow: dirs.OutFollow,
			logPath:      logOutPath,
		}
		if err = c.init(); err != nil {
			panic(err)
		}
		a.clients = append(a.clients, c)
		watch := c.outPath
		if _, ok := watching[watch]; ok {
			panic("Multiple sources configured to watch the same outgoing directory")
		}
		watching[watch] = true
	}
	for _, c := range a.clients {
		stop := make(chan bool)
		done := make(chan bool)
		a.clientStop[stop] = done
		go c.broker.Start(stop, done)
	}
	return true
}

func (a *app) stopClients() {
	if len(a.clients) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(a.clients))
	for stop, done := range a.clientStop {
		go func(stop chan<- bool, done <-chan bool, wg *sync.WaitGroup) {
			defer wg.Done()
			log.Debug("Stopping client...")
			stop <- true
			log.Debug("Waiting for client...")
			<-done
		}(stop, done, &wg)
	}
	wg.Wait()
}
