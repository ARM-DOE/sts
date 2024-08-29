package main

import (
	"flag"
	"fmt"
	golog "log"
	"runtime"
	"sync"
	"syscall"
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

	httpbase "net/http"

	"github.com/coreos/go-systemd/v22/daemon"
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

func getVersionString() string {
	return fmt.Sprintf("%s (%s) @ %s", Version, runtime.Version(), BuildTime)
}

func main() {
	if ConfigServer != "" {
		runFromServer(ConfigServer)
	} else {
		newApp().run()
	}
}

type app struct {
	debug        bool
	loop         bool
	mode         string
	root         string
	confPath     string
	conf         *sts.Conf
	clients      []*clientApp
	isRunning    bool
	isRunningMux sync.RWMutex
	serverStop   chan<- bool
	serverDone   <-chan bool
	clientStop   map[chan<- bool]<-chan bool
	iServerStop  chan<- bool
	iServerDone  <-chan bool
	iPort        int
}

func newApp() *app {
	var err error

	a := appFromCLI()

	err = sts.InitPaths(
		a.conf,
		filepath.Join,
		func(path *string, isDir bool) error {
			*path, err = fileutil.InitPath(a.root, *path, isDir)
			return err
		})
	if err != nil {
		golog.Fatalln("Failed to initialize paths:", err.Error())
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
	rootPath := flag.String(
		"root",
		os.Getenv("STS_HOME"),
		strings.Join([]string{
			"Root path for both finding the configuration file",
			"as well as for evaluating relative paths within",
			"the configuration (defaults to $STS_HOME)",
		}, "\n"),
	)
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
		fmt.Println(getVersionString())
		os.Exit(0)
	}

	a := &app{
		debug:    *debug,
		mode:     strings.ToLower(*mode),
		loop:     *loop,
		confPath: *confPath,
		root:     *rootPath,
		iPort:    *internalPort,
	}

	if a.root == "" {
		ex, err := os.Executable()
		if err != nil {
			golog.Fatalln("Failed to find executable path:", err.Error())
		}
		a.root = filepath.Dir(ex)
	} else if !filepath.IsAbs(a.root) {
		a.root, err = filepath.Abs(a.root)
		if err != nil {
			golog.Fatalln("Failed to resolve root path:", err.Error())
		}
	}

	// Parse configuration
	if a.confPath == "" {
		attempts := []string{
			filepath.Join(a.root, "sts"),
			filepath.Join(a.root, "conf", "sts"),
		}
		if a.mode != modeAuto {
			for _, p := range attempts {
				attempts = append(attempts, fmt.Sprintf("%s.%s", p, a.mode))
			}
		}
		attempted := []string{}
		for _, p := range attempts {
			for _, ext := range []string{"yaml", "json"} {
				path := fmt.Sprintf("%s.%s", p, ext)
				if _, err := os.Stat(path); err == nil {
					a.confPath = path
					break
				}
				attempted = append(attempted, path)
			}
		}
		if a.confPath == "" {
			golog.Fatalln("Failed to find configuration file:",
				strings.Join(attempted, " OR\n- "))
		}
	} else if !filepath.IsAbs(a.confPath) {
		// Find the configuration based on the relative path provided
		if a.confPath, err = filepath.Abs(a.confPath); err != nil {
			golog.Fatalln("Failed to resolve configuration path:", err.Error())
		}
	}

	// Read the local configuration file
	if a.conf, err = sts.NewConf(a.confPath); err != nil {
		golog.Fatalln("Failed to parse configuration:", err.Error())
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
		golog.Fatalln("Not configured to do anything?")
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
		signal.Notify(sc, os.Interrupt, syscall.SIGTERM)

		if _, err := daemon.SdNotify(false, daemon.SdNotifyReady); err != nil {
			log.Error("Failed to notify systemd:", err.Error())
		}

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
						if _, err = daemon.SdNotify(false, daemon.SdNotifyWatchdog); err != nil {
							log.Error("Failed to notify watchdog:", err.Error())
						}
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
		if _, err := daemon.SdNotify(false, daemon.SdNotifyStopping); err != nil {
			log.Error("Failed to notify systemd:", err.Error())
		}
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
		golog.Fatalln("Server configuration missing")
	}
	s := &serverApp{
		debug: a.debug,
		conf:  a.conf.Server,
	}
	if err = s.init(); err != nil {
		golog.Fatalln("Failed to initialize server:", err.Error())
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
	if a.conf == nil ||
		a.conf.Client == nil ||
		a.conf.Client.Dirs == nil {
		if a.mode == modeAuto {
			return false
		}
		golog.Fatalln("Source client configuration missing")
	}
	a.isRunningMux.Lock()
	defer a.isRunningMux.Unlock()
	var err error
	dirs := a.conf.Client.Dirs
	log.Init(dirs.LogMsg, a.debug, nil, nil)
	clientsByWatching := make(map[string]*clientApp)
	for _, source := range a.conf.Client.Sources {
		c := &clientApp{
			conf:         source,
			dirCache:     dirs.Cache,
			dirOutFollow: dirs.OutFollow,
		}
		if err = c.init(); err != nil {
			log.Error("Failed to initialize source client %s: %s",
				c.conf.Name, err.Error())
			continue
		}
		clientsByWatching[c.conf.OutDir] = c
	}
	a.clients = []*clientApp{}
	for d0, c0 := range clientsByWatching {
		overlap := false
		for d1, c1 := range clientsByWatching {
			if c0 == c1 {
				continue
			}
			if d0 == d1 || strings.HasPrefix(d0, d1) || strings.HasPrefix(d1, d0) {
				overlap = true
				log.Error("Outgoing directory overlaps are not allowed:\n - %s:%s\n - %s:%s",
					c0.conf.Name, d0, c1.conf.Name, d1)
			}
		}
		if !overlap {
			a.clients = append(a.clients, c0)
		}
	}
	if len(a.clients) == 0 {
		log.Error("No source clients to start")
		return false
	}
	a.clientStop = make(map[chan<- bool]<-chan bool)
	for _, c := range a.clients {
		stop := make(chan bool)
		done := make(chan bool)
		a.clientStop[stop] = done
		go c.broker.Start(stop, done)
	}
	a.isRunning = true
	return true
}

func (a *app) stopClients(stopGraceful bool) {
	a.isRunningMux.Lock()
	a.isRunning = false
	defer a.isRunningMux.Unlock()
	if len(a.clients) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(a.clientStop))
	for stop, done := range a.clientStop {
		go func(stop chan<- bool, graceful bool, done <-chan bool, wg *sync.WaitGroup) {
			defer wg.Done()
			log.Debug("Stopping client...")
			stop <- graceful
			log.Debug("Waiting for client...")
			<-done
		}(stop, stopGraceful, done, &wg)
	}
	wg.Wait()
	for _, c := range a.clients {
		c.destroy()
	}
	a.clients = nil
	a.clientStop = nil
}

func (a *app) startInternalServer() {
	if a.iPort == 0 {
		return
	}
	fmt.Println("Starting internal server...", a.iPort)
	server := &http.Internal{
		Port: a.iPort,
		Handlers: map[string]httpbase.HandlerFunc{
			"restart-clients": a.restartHandler,
		},
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

func (a *app) restartHandler(w httpbase.ResponseWriter, r *httpbase.Request) {
	log.Debug("Restarting clients...")
	switch r.Method {
	case httpbase.MethodPut:
		fallthrough
	case httpbase.MethodPost:
		a.stopClients(false)
		a.startClients()
		w.WriteHeader(httpbase.StatusOK)
	default:
		w.WriteHeader(httpbase.StatusBadRequest)
	}
}
