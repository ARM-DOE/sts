package main

import (
	"encoding/json"
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

func runFromServer(jsonEncodedServerConfig string) {
	log.InitExternal(&logger{
		debugMode: true,
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
		TargetHost: host,
		TargetPort: port,
		TLS:        tls,
	}

	addr, err := getMacAddr()
	if err != nil {
		panic(err)
	}
	clientID := serverConf.Key + ":" + fileutil.StringMD5(strings.Join(addr, ""))

	log.Debug("Client ID:", clientID)

	ch := make(chan *sts.ClientConf)

	go requestClientConf(httpClient, clientID, ch)

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
		conf: &sts.Conf{
			Client: <-ch,
		},
	}

	a.runControlledClients(ch)
}

// runControlledClients runs in a loop waiting for any config changes to be
// detected or a signal to be received.  If the former, running clients are
// stopped (gracefully) and then restarted using the new configuration.
func (a *app) runControlledClients(confCh <-chan *sts.ClientConf) {
	var err error
	signalCh := make(chan os.Signal)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	for {
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

		log.Debug("Starting clients...")
		a.startClients()

	wait:
		select {
		case <-signalCh:
			log.Debug("Shutting down clients...")
			a.stopClients(false)
			return
		case a.conf.Client = <-confCh:
			log.Debug("Stopping clients...")
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

// requestClientConf runs in a loop checking the status of this client and
// getting updated configuration from the server as needed
func requestClientConf(
	manager sts.ClientManager,
	clientID string,
	ch chan<- *sts.ClientConf,
) {
	var err error
	var name string
	var status sts.ClientStatus
	var conf *sts.ClientConf
	heartbeat := time.Second * 30
	if StatusInterval != "" {
		if nSec, err := strconv.Atoi(StatusInterval); err == nil {
			heartbeat = time.Second * time.Duration(nSec)
		}
	}
	log.Debug("Heartbeat:", heartbeat)
	for {
		log.Debug("Getting client status:", clientID)
		if name, err = os.Hostname(); err != nil {
			log.Error(err.Error())
		}
		status, err = manager.GetClientStatus(clientID, name, runtime.GOOS)
		if err != nil {
			log.Debug(err.Error())
			goto next
		}
		switch {
		case status&sts.ClientIsDisabled != 0:
			log.Debug("Client disabled:", clientID)
			goto next
		case status&sts.ClientIsApproved != 0 && status&sts.ClientHasUpdatedConfiguration != 0:
			log.Debug("Client has updated conf:", clientID)
			if conf, err = manager.GetClientConf(clientID); err != nil {
				log.Error(err.Error())
				goto next
			}
			if conf != nil && len(conf.Sources) > 0 {
				log.Debug("Applying updated client conf:", clientID)
				ch <- conf
			}
		}
	next:
		time.Sleep(heartbeat)
	}
}
