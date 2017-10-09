package main

import (
	"flag"
	"fmt"

	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/log"
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
	server     *serverApp
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
	logPath, err := fileutil.InitPath(a.root, a.conf.Server.Dirs.Logs, true)
	if err != nil {
		panic(err)
	}
	log.Init(logPath, a.debug)
	a.server = &serverApp{
		root: a.root,
		conf: a.conf.Server,
	}
	if err := a.server.init(); err != nil {
		panic(err)
	}
	stop := make(chan bool)
	done := make(chan bool)
	a.serverStop = stop
	a.serverDone = done
	go a.server.http.Serve(stop, done)
	return true
}

func (a *app) stopServer() {
	if a.serverStop != nil {
		a.serverStop <- true
	}
	if a.serverDone != nil {
		<-a.serverDone
	}
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
	logPath, err := fileutil.InitPath(a.root, a.conf.Client.Dirs.Logs, true)
	if err != nil {
		panic(err)
	}
	log.Init(logPath, a.debug)
	a.clientStop = make(map[chan<- bool]<-chan bool)
	watching := make(map[string]bool)
	for _, source := range a.conf.Client.Sources {
		c := &clientApp{
			root: a.root,
			dirs: a.conf.Client.Dirs,
			conf: source,
		}
		if err = c.init(); err != nil {
			panic(err)
		}
		a.clients = append(a.clients, c)
		watch := c.outDir
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
	for stop, done := range a.clientStop {
		stop <- true
		<-done
	}
}
