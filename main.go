package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/ARM-DOE/sts/logging"
)

func main() {
	app := newApp()
	app.run()
}

// getRoot returns the STS root.  It will use $STS_DATA and fall back to the directory
// of the executable plus "/sts".
func getRoot() string {
	root := os.Getenv("STS_DATA")
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
	// Initialize command line arguments
	help := flag.Bool("help", false, "Print the help message")
	debug := flag.Bool("debug", false, "Log program flow")
	mode := flag.String("mode", "send", "Mode: 'send' or 'receive' or 'both'")
	loop := flag.Bool("loop", false, "Run in a loop (applies to send mode only)")
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
		a.confPath = filepath.Join(getRoot(), "conf", "dist."+a.mode+".yaml")
	}
	conf, err := NewConf(a.confPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse configuration: %s\n%s", *confPath, err.Error()))
	}
	a.conf = conf

	return a
}

func (a *app) run() {
	a.startIn()
	a.startOut(a.once)

	if !a.once { // Run until we get a signal to shutdown.
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, os.Interrupt)

		logging.Debug("Waiting for signal...")

		<-sc // Block until we get a signal.

		// Shutdown gracefully.
		a.stopOut()
	}
	a.doneOut()
	a.stopIn()
	a.doneIn()
}

func (a *app) startIn() {
	if a.mode != "receive" && a.mode != "both" {
		return
	}
	if a.conf.Receive == nil || a.conf.Receive.Dirs == nil || a.conf.Receive.Server == nil {
		panic("Missing required RECEIVER configuration")
	}
	logging.Init([]string{logging.Receive, logging.Msg}, InitPath(a.root, a.conf.Receive.Dirs.Logs, true), a.debug)
	a.in = &AppIn{
		root:    a.root,
		rawConf: a.conf.Receive,
	}
	stop := make(chan bool)
	a.inStop = stop
	a.inDone = a.in.Start(stop)
	time.Sleep(1 * time.Second) // Give it time to start the server.
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

func (a *app) startOut(once bool) {
	if a.mode != "send" && a.mode != "both" {
		return
	}
	if a.conf.Send == nil || a.conf.Send.Sources == nil || len(a.conf.Send.Sources) < 1 {
		panic("Missing required SENDER configuration")
	}
	logging.Init([]string{logging.Send, logging.Msg}, InitPath(a.root, a.conf.Send.Dirs.Logs, true), a.debug)
	a.outStop = make(map[chan<- bool]<-chan bool)
	for _, source := range a.conf.Send.Sources {
		out := &AppOut{
			root:    a.root,
			dirConf: a.conf.Send.Dirs,
			rawConf: source,
		}
		c := make(chan bool)
		if once {
			a.outStop[c] = out.Start(nil)
		} else {
			a.outStop[c] = out.Start(c)
		}
		a.out = append(a.out, out)
	}
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
