package main

import (
	"flag"
	"os"

	"github.com/ARM-DOE/sts/receiver"
	"github.com/ARM-DOE/sts/sender"
	"github.com/ARM-DOE/sts/util"
)

func main() {
	// Initialize command line arguments
	help := flag.Bool("help", false, "Print the help message")
	debug := flag.Bool("debug", false, "Log program flow")
	mode := flag.String("mode", "", "Mode: 'send' or 'receive'")
	conf := flag.String("conf", "", "Configuration file path")

	// Parse command line
	flag.Parse()

	// Validate
	if *help || (*mode != "send" && *mode != "receive") {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Parse configuration and initialize directories
	if *conf == "" {
		*conf = util.JoinPath(util.GetRootPath(), "conf", "dist."+*mode+".yaml")
	}
	config, err := util.ParseConfig(*conf)
	if err != nil {
		util.LogError("Failed to parse configuration: "+*conf, err.Error())
		os.Exit(1)
	}
	config.InitPaths()

	// Initialize global logging
	logger := util.NewLogger(config.Logs_Directory, util.LOGGING_MSG, *debug)
	util.InitLogger(logger)

	// Run
	if *mode == "send" {
		sender.Main(config)
	} else {
		receiver.Main(config)
	}
}
