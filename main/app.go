package main

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	stackimpact "github.com/stackimpact/stackimpact-go"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"

	// For profiling...
	// "net/http"
	// _ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/coreos/go-systemd/daemon"
)

const modeSend = "out"
const modeRecv = "in"
const modeAuto = "auto"

var (
	// Version is set based on -X option passed at build.
	Version = ""
	// BuildTime is set based on -X option passed at build.
	BuildTime = ""
	// ConfigServer is set based on -X option passed at build.  It is expected
	// to be a JSON-encoded string that can be decoded as an sts.TargetConf
	// struct
	ConfigServer = ""
)

func main() {
	if ConfigServer != "" {
		runFromServer(ConfigServer)
	} else {
		newApp().run()
	}
}

type app struct {
	debug       bool
	loop        bool
	mode        string
	root        string
	confPath    string
	conf        *sts.Conf
	clients     []*clientApp
	serverStop  chan<- bool
	serverDone  <-chan bool
	clientStop  map[chan<- bool]<-chan bool
	iServerStop chan<- bool
	iServerDone <-chan bool
	iPort       int
}

func newApp() *app {
	var err error
	var a *app

	a = appFromCLI()

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

func appFromCLI() *app {
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
	internalPort := flag.Int("iport", 0,
		"TCP port for internal command server")

	// Parse command line
	flag.Parse()

	// Validate
	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *vers {
		fmt.Printf("%s (%s) @ %s\n", Version, runtime.Version(), BuildTime)
		os.Exit(0)
	}

	a := &app{
		debug:    *debug,
		mode:     strings.ToLower(*mode),
		loop:     *loop,
		confPath: *confPath,
		root:     os.Getenv("STS_HOME"),
		iPort:    *internalPort,
	}

	// Parse configuration
	if a.confPath == "" {
		if a.root != "" {
			// Find the configuration file based on $STS_HOME
			if a.mode == modeAuto {
				a.confPath = filepath.Join(a.root, "conf", "sts.yaml")
			} else {
				a.confPath = filepath.Join(a.root, "conf", "sts."+a.mode+".yaml")
			}
		}
	} else if !filepath.IsAbs(a.confPath) {
		// Find the configuration based on the full path provided
		if a.confPath, err = filepath.Abs(a.confPath); err != nil {
			panic(err.Error())
		}
		if a.root == "" {
			a.root = filepath.Dir(a.confPath)
		}
	}

	// Fall back to JSON if the YAML file doesn't exist
	if _, err := os.Stat(a.confPath); os.IsNotExist(err) {
		a.confPath = strings.TrimSuffix(a.confPath, filepath.Ext(a.confPath)) + ".json"
	}

	// Read the local configuration file
	if a.conf, err = sts.NewConf(a.confPath); err != nil {
		panic(fmt.Sprintf("Failed to parse configuration:\n%s", err.Error()))
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
	a.startInternalServer()
	i := a.startServer()
	o := a.startClients()
	if !i && !o {
		panic("Not configured to do anything?")
	}
	// Run until we get a signal to shutdown if running ONLY as a "receiver" or
	// if configured to run as a sender in a loop.
	isDaemon := a.loop || (i && !o)
	stopFull := !isDaemon
	if isDaemon {
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

		daemon.SdNotify(false, daemon.SdNotifyReady)

		var dogTicker *time.Ticker
		var dogDone chan bool
		dogInterval, err := daemon.SdWatchdogEnabled(false)
		if err == nil && dogInterval > 0 {
			dogTicker = time.NewTicker(dogInterval / 3)
			dogDone = make(chan bool)
			go func() {
				for {
					select {
					case <-dogDone:
						return
					case <-dogTicker.C:
						daemon.SdNotify(false, daemon.SdNotifyWatchdog)
					}
				}
			}()
		}

		log.Info("Startup complete. Running...")

		<-sc // Block until we get a signal

		if dogTicker != nil {
			// Stop the watchdog goroutine
			dogTicker.Stop()
			dogDone <- true
		}
		daemon.SdNotify(false, daemon.SdNotifyStopping)
	}
	a.stopClients(stopFull)
	a.stopServer()
	a.stopInternalServer()
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
	s := &serverApp{
		debug: a.debug,
		conf:  a.conf.Server,
	}
	if err = s.init(); err != nil {
		panic(err)
	}
	stop := make(chan bool)
	done := make(chan bool)
	a.serverStop = stop
	a.serverDone = done
	go s.server.Serve(stop, done)
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
	a.clients = []*clientApp{}
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

func (a *app) stopClients(stopFull bool) {
	if len(a.clients) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(a.clients))
	for stop, done := range a.clientStop {
		go func(stop chan<- bool, full bool, done <-chan bool, wg *sync.WaitGroup) {
			defer wg.Done()
			log.Debug("Stopping client...")
			stop <- full
			log.Debug("Waiting for client...")
			<-done
		}(stop, stopFull, done, &wg)
	}
	wg.Wait()
	a.clients = nil
	a.clientStop = nil
}

func (a *app) startInternalServer() {
	if a.iPort == 0 {
		return
	}
	server := &http.Internal{
		Port: a.iPort,
	}
	stop := make(chan bool)
	done := make(chan bool)
	a.iServerStop = make(chan bool)
	a.iServerDone = make(chan bool)
	go server.Serve(stop, done)
}

func (a *app) stopInternalServer() {
	if a.iServerStop == nil {
		return
	}
	a.iServerStop <- true
	<-a.iServerDone
}
