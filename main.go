package main

import (
	"flag"
	"fmt"
	"time"
	// For profiling...
	// "net/http"
	// _ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/ARM-DOE/sts/logging"
)

const modeSend = "out"
const modeRecv = "in"
const modeAuto = "auto"

func main() {
	app := newApp()
	app.run()
}

// getRoot returns the STS root.  It will use $STS_HOME and fall back to the directory
// of the executable plus "/sts".
func getRoot() string {
	root := os.Getenv("STS_HOME")
	if root == "" {
		var err error
		root, err = filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			root = string(os.PathSeparator) + "sts"
		}
	}
	return root
}

// InitPath will turn a relative path into absolute (based on root) and make sure it exists.
func InitPath(root string, path string, isdir bool) string {
	var err error
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(filepath.Join(root, path))
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
		logging.Debug("MAIN Make Path:", pdir)
		os.MkdirAll(pdir, os.ModePerm)
	}
	return path
}

type app struct {
	debug    bool
	once     bool
	mode     string
	root     string
	confPath string
	conf     *Conf
	in       *AppIn
	out      []*AppOut
	inStop   chan<- bool
	inDone   <-chan bool
	outStop  map[chan<- bool]<-chan bool
}

func newApp() *app {
	var err error

	// Initialize command line arguments
	help := flag.Bool("help", false, "Print the help message")
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
	conf, err := NewConf(a.confPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse configuration:\n%s", err.Error()))
	}
	a.conf = conf

	return a
}

func (a *app) run() {
	i := a.startIn()
	o := a.startOut(a.once)
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

		logging.Debug("Waiting for signal...")

		<-sc // Block until we get a signal.

		// Shutdown gracefully.
		a.stopOut()
	}
	a.doneOut()
	a.stopIn()
	a.doneIn()
}

func (a *app) startIn() bool {
	if a.mode != modeRecv && a.mode != modeAuto {
		return false
	}
	if a.conf.In == nil || a.conf.In.Dirs == nil || a.conf.In.Server == nil {
		if a.mode == modeAuto {
			return false
		}
		panic("Missing required RECEIVER configuration")
	}
	logging.Init(map[string]string{
		logging.In:  a.conf.In.Dirs.LogsIn,
		logging.Msg: a.conf.In.Dirs.LogsMsg,
	}, InitPath(a.root, a.conf.In.Dirs.Logs, true), a.debug)
	a.in = &AppIn{
		root:    a.root,
		rawConf: a.conf.In,
	}
	stop := make(chan bool)
	a.inStop = stop
	a.inDone = a.in.Start(stop)
	return true
}

func (a *app) stopIn() {
	if a.inStop != nil {
		a.inStop <- true
	}
}

func (a *app) doneIn() {
	if a.inDone != nil {
		<-a.inDone
	}
}

func (a *app) startOut(once bool) bool {
	if a.mode != modeSend && a.mode != modeAuto {
		return false
	}
	if a.conf.Out == nil || a.conf.Out.Sources == nil || len(a.conf.Out.Sources) < 1 {
		if a.mode == modeAuto {
			return false
		}
		panic("Missing required SENDER configuration")
	}
	logging.Init(map[string]string{
		logging.Out: a.conf.Out.Dirs.LogsOut,
		logging.Msg: a.conf.Out.Dirs.LogsMsg,
	}, InitPath(a.root, a.conf.Out.Dirs.Logs, true), a.debug)
	a.outStop = make(map[chan<- bool]<-chan bool)
	for _, source := range a.conf.Out.Sources {
		out := &AppOut{
			root:    a.root,
			dirConf: a.conf.Out.Dirs,
			rawConf: source,
		}
		out.Init()
		a.out = append(a.out, out)
	}
	started := make([]bool, len(a.conf.Out.Sources))
	watching := make(map[string]bool)
	nloops := 0
	for {
		nerr := 0
		for i := range a.conf.Out.Sources {
			if started[i] {
				continue
			}
			if err := a.out[i].Recover(); err != nil {
				logging.Error(err.Error())
				nerr++
				continue
			}
			c := make(chan bool)
			if once {
				a.outStop[c] = a.out[i].Start(nil)
			} else {
				a.outStop[c] = a.out[i].Start(c)
			}
			started[i] = true
			watch := a.out[i].scanner.conf.ScanDir
			if _, ok := watching[watch]; ok {
				panic("Multiple sources configured to watch the same outgoing directory")
			}
			watching[watch] = true
		}
		if !once && nerr > 0 {
			nloops++
			time.Sleep(time.Duration(nloops) * time.Second) // Wait longer the more it fails.
			continue
		}
		break
	}
	return true
}

func (a *app) stopOut() {
	if a.outStop != nil {
		for stop := range a.outStop {
			stop <- true
		}
	}
}

func (a *app) doneOut() {
	if a.outStop != nil {
		for _, done := range a.outStop {
			<-done
		}
	}
}
