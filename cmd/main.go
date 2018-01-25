package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"sync"

	stackimpact "github.com/stackimpact/stackimpact-go"

	"code.arm.gov/dataflow/sts"
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

func main() {
	app := newApp()
	app.run()
}

type app struct {
	debug      bool
	loop       bool
	mode       string
	root       string
	confPath   string
	conf       *sts.Conf
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
	mode := flag.String("mode", modeAuto,
		fmt.Sprintf("Mode: \"%s\", \"%s\", \"%s\"",
			modeSend, modeRecv, modeAuto))
	loop := flag.Bool("loop", false,
		"Run in a loop, i.e. don't exit until interrupted")
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
		loop:     *loop,
		confPath: *confPath,
		root:     os.Getenv("STS_HOME"),
	}

	// Parse configuration
	if a.confPath == "" {
		if a.root == "" {
			panic("Cannot find configuration file")
		}
		if a.mode == modeAuto {
			a.confPath = filepath.Join(a.root, "conf", "sts.yaml")
		} else {
			a.confPath = filepath.Join(a.root, "conf", "sts."+a.mode+".yaml")
		}
	} else if !filepath.IsAbs(a.confPath) {
		if a.confPath, err = filepath.Abs(a.confPath); err != nil {
			panic(err.Error())
		}
	}
	if a.root == "" {
		a.root = filepath.Dir(a.confPath)
	}
	conf, err := sts.NewConf(a.confPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse configuration:\n%s", err.Error()))
	}
	a.conf = conf
	err = sts.InitPaths(
		a.conf,
		filepath.Join,
		func(path *string, isDir bool) error {
			*path, err = fileutil.InitPath(a.root, *path, isDir)
			return err
		})
	if err != nil {
		panic(err.Error())
	}

	return a
}

func (a *app) run() {
	if a.conf.AgentKey != "" {
		agent := stackimpact.Start(stackimpact.Options{
			AgentKey:   a.conf.AgentKey,
			AppName:    "STS",
			AppVersion: Version,
			Debug:      a.debug,
		})
		span := agent.Profile()
		defer span.Stop()
	}
	i := a.startServer()
	o := a.startClients()
	if !i && !o {
		panic("Not configured to do anything?")
	}
	// Run until we get a signal to shutdown if running ONLY as a "receiver" or
	// if configured to run as a sender in a loop.
	if a.loop || (i && !o) {
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
	var err error
	if a.mode != modeRecv && a.mode != modeAuto {
		return false
	}
	if a.conf.Server == nil ||
		a.conf.Server.Dirs == nil ||
		a.conf.Server.Server == nil {
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
	log.Init(dirs.LogMsg, a.debug)
	newStage := func(source string) sts.GateKeeper {
		return stage.New(
			source,
			filepath.Join(dirs.Stage, source),
			filepath.Join(dirs.Final, source),
			log.NewReceive(filepath.Join(dirs.LogIn, source)))
	}
	nodes, err := ioutil.ReadDir(dirs.Stage)
	if err != nil {
		panic(err)
	}
	var stager sts.GateKeeper
	var stagers map[string]sts.GateKeeper
	stagers = make(map[string]sts.GateKeeper)
	for _, node := range nodes {
		if node.IsDir() {
			stager = newStage(node.Name())
			if err = stager.Recover(); err != nil {
				panic(err)
			}
			stagers[node.Name()] = stager
		}
	}
	server := &http.Server{
		ServeDir:          dirs.Serve,
		Host:              conf.Server.Host,
		Port:              conf.Server.Port,
		Sources:           conf.Sources,
		Keys:              conf.Keys,
		Compression:       conf.Server.Compression,
		DecoderFactory:    payload.NewDecoder,
		GateKeepers:       stagers,
		GateKeeperFactory: newStage,
	}
	if conf.Server.TLSCertPath != "" && conf.Server.TLSKeyPath != "" {
		if server.TLS, err = http.GetTLSConf(
			conf.Server.TLSCertPath,
			conf.Server.TLSKeyPath, ""); err != nil {
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
	if a.conf.Client == nil ||
		a.conf.Client.Sources == nil ||
		len(a.conf.Client.Sources) < 1 {
		if a.mode == modeAuto {
			return false
		}
		panic("Missing required CLIENT (aka SENDER) configuration")
	}
	var err error
	dirs := a.conf.Client.Dirs
	log.Init(dirs.LogMsg, a.debug)
	a.clientStop = make(map[chan<- bool]<-chan bool)
	watching := make(map[string]bool)
	for _, source := range a.conf.Client.Sources {
		c := &clientApp{
			conf:         source,
			dirCache:     dirs.Cache,
			dirOutFollow: dirs.OutFollow,
		}
		if err = c.init(); err != nil {
			panic(err)
		}
		a.clients = append(a.clients, c)
		watch := c.conf.OutDir
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
