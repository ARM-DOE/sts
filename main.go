package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ARM-DOE/sts/logging"
)

func main() {
	app := newApp()
	app.start()
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
	mode     string
	root     string
	confPath string
	conf     *Conf
	in       *AppIn
	out      []*AppOut
}

func newApp() *app {
	// Initialize command line arguments
	help := flag.Bool("help", false, "Print the help message")
	debug := flag.Bool("debug", false, "Log program flow")
	mode := flag.String("mode", "", "Mode: 'send' or 'receive'")
	confPath := flag.String("conf", "", "Configuration file path")

	// Parse command line
	flag.Parse()

	// Validate
	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Parse configuration and initialize directories
	if *confPath == "" {
		*confPath = filepath.Join(getRoot(), "conf", "dist."+*mode+".yaml")
	}
	conf, err := NewConf(*confPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse configuration: %s\n%s", *confPath, err.Error()))
	}

	a := &app{
		debug:    *debug,
		mode:     *mode,
		confPath: *confPath,
		conf:     conf,
		root:     getRoot(),
	}
	return a
}

func (a *app) start() {
	if a.mode == "receive" || a.mode == "" {
		a.startIn()
	}
	if a.mode == "send" || a.mode == "" {
		a.startOut()
	}

	// TODO: Add mechanism for graceful shutdown by sending "empty" data down the input channels
	// signaling to channel listeners to stop listening.  See documentation on Go pipelines.
	for {
		// This is a daemon, so keep on running.
		time.Sleep(5 * time.Second)
	}
}

func (a *app) startIn() {
	if a.conf.Receive == nil || a.conf.Receive.Dirs == nil || a.conf.Receive.Server == nil {
		panic("Missing required RECEIVER configuration")
	}
	logging.Init([]string{logging.Receive, logging.Msg}, InitPath(a.root, a.conf.Receive.Dirs.Logs, true), a.debug)
	a.in = &AppIn{
		root:    a.root,
		rawConf: a.conf.Receive,
	}
	a.in.Start()
	time.Sleep(1 * time.Second) // Give it time to start the server.
}

func (a *app) startOut() {
	if a.conf.Send == nil || a.conf.Send.Sources == nil || len(a.conf.Send.Sources) < 1 {
		panic("Missing required SENDER configuration")
	}
	logging.Init([]string{logging.Send, logging.Msg}, InitPath(a.root, a.conf.Send.Dirs.Logs, true), a.debug)
	for _, source := range a.conf.Send.Sources {
		out := &AppOut{
			root:    a.root,
			dirConf: a.conf.Send.Dirs,
			rawConf: source,
		}
		out.Start()
		a.out = append(a.out, out)
	}
}
