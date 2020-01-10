package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
)

// StatusInterval is for configuring how many seconds to wait between checking
// the client's status from the server.  The value is a string in order to be
// configured at compile-time.
var StatusInterval = ""

type logger struct {
	debugMode bool
}

// Debug logs debug messages
func (log *logger) Debug(params ...interface{}) {
	if log.debugMode {
		fmt.Fprintln(os.Stdout, params...)
	}
}

// Info logs general information
func (log *logger) Info(params ...interface{}) {
	fmt.Fprintln(os.Stdout, params...)
}

// Error logs ...errors
func (log *logger) Error(params ...interface{}) {
	fmt.Fprintln(os.Stderr, params...)
}

// Sent fulfills the sts.SendLogger interface
func (log *logger) Sent(file sts.Sent) {
}

// WasSent fulfills the sts.SendLogger interface
func (log *logger) WasSent(name string, after time.Time, before time.Time) bool {
	return false
}

func encodeClientID(key, uid string) string {
	return key + ":" + uid
}

func decodeClientID(clientID string) (string, string) {
	parts := strings.Split(clientID, ":")
	if len(parts) < 2 {
		return clientID, ""
	}
	return parts[0], parts[1]
}

func runFromServer(jsonEncodedServerConfig string) {
	var err error

	help := flag.Bool("help", false, "Print the help message")
	vers := flag.Bool("version", false, "Print version information")
	once := flag.Bool("once", false, "Run a single scan/send loop and then quit")
	verbose := flag.Bool("verbose", false, "Output detailed logic flow messages")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *vers {
		fmt.Printf("%s (%s) @ %s\n", Version, runtime.Version(), BuildTime)
		os.Exit(0)
	}

	log.InitExternal(&logger{
		debugMode: *verbose,
	})

	bytes := []byte(jsonEncodedServerConfig)
	serverConf := sts.TargetConf{}
	if err := json.Unmarshal(bytes, &serverConf); err != nil {
		panic(err)
	}

	tls, err := getTLSConf(&serverConf)
	if err != nil {
		panic(err)
	}
	host, port, err := serverConf.ParseHost()
	if err != nil {
		panic(err)
	}
	httpClient := &http.Client{
		TargetHost:   host,
		TargetPort:   port,
		TargetPrefix: serverConf.PathPrefix,
		TLS:          tls,
	}

	addr, err := getMacAddr()
	if err != nil {
		panic(err)
	}
	clientID := encodeClientID(
		serverConf.Key,
		fileutil.StringMD5(strings.Join(addr, "")),
	)

	log.Debug("Server:", fmt.Sprintf("%s:%d", host, port))
	log.Debug("Server Prefix:", serverConf.PathPrefix)
	log.Debug("Client ID:", clientID)

	rootDir := os.Getenv("STS_HOME")
	if rootDir == "" {
		rootDir, err = os.UserCacheDir()
		if err != nil {
			panic(err)
		}
		rootDir = filepath.Join(rootDir, "sts")
	}

	a := &app{
		loop: true,
		mode: modeSend,
		root: rootDir,
		conf: &sts.Conf{},
	}

	if *once {
		if a.conf.Client, err = requestClientConf(httpClient, clientID); err != nil {
			log.Error(err.Error())
			return
		}
		if a.conf.Client != nil {
			a.runControlledClients(nil)
		}
	} else {
		ch := make(chan *sts.ClientConf)
		go runConfLoader(httpClient, clientID, ch)
		a.conf.Client = <-ch
		a.runControlledClients(ch)
	}

	log.Debug("Done.")
}

// runControlledClients starts the configured client(s) and, if the input
// channel is not nil, will run in a loop waiting for any config changes to be
// detected or a signal to be received.  If the former, running clients are
// stopped (gracefully) and then restarted using the new configuration.  If the
// input channel is nil, the client(s) will be immediately stopped once they
// have performed a single send iteration.
func (a *app) runControlledClients(confCh <-chan *sts.ClientConf) {
	var err error
	signalCh := make(chan os.Signal)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	for {
		if a.conf.Client.Dirs == nil {
			a.conf.Client.Dirs = &sts.ClientDirs{}
		}
		if a.conf.Client.Dirs.Cache == "" {
			a.conf.Client.Dirs.Cache = "cache"
		}
		err = sts.InitPaths(
			a.conf,
			filepath.Join,
			func(path *string, isDir bool) error {
				*path, err = fileutil.InitPath(a.root, *path, isDir)
				return err
			})
		if err != nil {
			log.Error(err.Error())
			goto wait
		}

		if len(a.conf.Client.Sources) > 0 {
			log.Debug("Starting Client(s)...")
			a.startClients()
		}

	wait:
		if confCh == nil {
			a.stopClients(true)
			return
		}
		select {
		case <-signalCh:
			log.Debug("Shutting Down Client(s)...")
			a.stopClients(false)
			return
		case a.conf.Client = <-confCh:
			log.Debug("Stopping Client(s)...")
			a.stopClients(false)
			continue
		}
	}
}

// getMacAddr returns the list of MAC addresses for all available interfaces
func getMacAddr() ([]string, error) {
	ifas, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var addr []string
	for _, ifa := range ifas {
		a := ifa.HardwareAddr.String()
		if a != "" {
			addr = append(addr, a)
		}
	}
	return addr, nil
}

// runConfLoader runs in a loop checking the status of this client and getting
// updated configuration from the server as needed
func runConfLoader(manager sts.ClientManager, clientID string, ch chan<- *sts.ClientConf) {
	var err error
	var conf *sts.ClientConf
	heartbeat := time.Second * 30
	if StatusInterval != "" {
		if nSec, err := strconv.Atoi(StatusInterval); err == nil {
			heartbeat = time.Second * time.Duration(nSec)
		}
	}
	log.Debug("Heartbeat:", heartbeat)
	for {
		if conf, err = requestClientConf(manager, clientID); err != nil {
			log.Error(err.Error())
		}
		if conf != nil {
			ch <- conf
		}
		time.Sleep(heartbeat)
	}
}

// requestClientConf checks the status of this client and gets updated
// configuration from the server if available
func requestClientConf(
	manager sts.ClientManager,
	clientID string,
) (conf *sts.ClientConf, err error) {
	var name string
	var status sts.ClientStatus
	log.Debug("Getting Client Status:", clientID)
	if name, err = os.Hostname(); err != nil {
		log.Error(err.Error())
	}
	status, err = manager.GetClientStatus(clientID, name, runtime.GOOS)
	if err != nil {
		return
	}
	switch {
	case status&sts.ClientIsDisabled != 0:
		log.Info("Client Disabled:", clientID)
		return
	case status&sts.ClientIsApproved != 0:
		if status&sts.ClientHasUpdatedConfiguration != 0 || conf == nil {
			log.Info("Client Has [Updated] Configuration:", clientID)
			if conf, err = manager.GetClientConf(clientID); err != nil {
				return
			}
			if conf == nil {
				err = fmt.Errorf("An Unknown Error Occurred - Check Client Configuration")
				return
			}
			if len(conf.Sources) == 0 {
				log.Info("No Data Sources Configured")
			}
		}
	}
	return
}
