package main

import (
	"encoding/json"
	"flag"
	"fmt"
	golog "log"
	"net"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
	"github.com/google/uuid"
	"github.com/postfinance/single"
)

var (
	// StatusInterval is for configuring how many seconds to wait between checking
	// the client's status from the server. The value is a string in order to be
	// configured at compile-time.
	StatusInterval = ""

	// StateInterval is for configuring how many seconds to wait between sending
	// the client's state to the server. The value is a string in order to be
	// configured at compile-time.
	StateInterval = ""
)

type logger struct {
	debugMode  bool
	mux        sync.RWMutex
	recentMsgs []string
	recentLen  int
}

func newLogger(debug bool) (lg *logger) {
	lg = &logger{
		debugMode: debug,
		recentLen: 10,
	}
	lg.recentMsgs = make([]string, lg.recentLen)
	return
}

func prependTimestamp(params ...interface{}) []interface{} {
	return append(
		[]interface{}{time.Now().UTC().Format(time.RFC3339)},
		params...,
	)
}

// Debug logs debug messages
func (log *logger) Debug(params ...interface{}) {
	if log.debugMode {
		fmt.Fprintln(os.Stdout, prependTimestamp(params)...)
	}
}

// Info logs general information
func (log *logger) Info(params ...interface{}) {
	params = prependTimestamp(params)
	log.remember(params...)
	fmt.Fprintln(os.Stdout, params...)
}

// Error logs ...errors
func (log *logger) Error(params ...interface{}) {
	params = prependTimestamp(params)
	log.remember(params...)
	fmt.Fprintln(os.Stderr, params...)
}

func (log *logger) remember(params ...interface{}) {
	log.mux.Lock()
	defer log.mux.Unlock()
	log.recentMsgs = append(log.recentMsgs, fmt.Sprint(params...))[1:]
}

// Recent implements sts.Logger
func (log *logger) Recent(n int) (msgs []string) {
	log.mux.RLock()
	defer log.mux.RUnlock()
	total := len(log.recentMsgs)
	offset := total - n
	if offset < 0 || n <= 0 {
		offset = 0
	}
	msgs = append(msgs, log.recentMsgs...)[offset:]
	return
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
		fmt.Println(getVersionString())
		os.Exit(0)
	}

	log.InitExternal(newLogger(*verbose))

	bytes := []byte(jsonEncodedServerConfig)
	serverConf := sts.TargetConf{}
	if err := json.Unmarshal(bytes, &serverConf); err != nil {
		golog.Fatalln("Internal JSON parsing error:", err.Error())
	}

	tls, err := getTLSConf(&serverConf)
	if err != nil {
		golog.Fatalln("Error getting TLS configuration:", err.Error())
	}
	host, port, err := serverConf.ParseHost()
	if err != nil {
		golog.Fatalln("Error parsing server host:", err.Error())
	}
	httpClient := &http.Client{
		TargetHost:   host,
		TargetPort:   port,
		TargetPrefix: serverConf.PathPrefix,
		TLS:          tls,
	}

	machineID, err := getMachineID(false)
	if err != nil {
		golog.Fatalln("Error getting machine ID:", err.Error())
	}
	clientID := encodeClientID(serverConf.Key, machineID)

	procLock, err := acquireProcLock(clientID)
	if err != nil {
		golog.Fatalln("Error acquiring process lock:", err.Error())
	}
	defer releaseProcLock(procLock)

	log.Info("Server:", fmt.Sprintf("%s:%d", host, port))
	log.Info("Server Prefix:", serverConf.PathPrefix)
	log.Info("Client ID:", clientID)

	rootDir := os.Getenv("STS_HOME")
	if rootDir == "" {
		rootDir, err = os.UserCacheDir()
		if err != nil {
			golog.Fatalln("Error getting user cache directory:", err.Error())
		}
		rootDir = filepath.Join(rootDir, "sts")
	}

	log.Info("Cache Directory:", rootDir)

	a := &app{
		mode: modeSend,
		root: rootDir,
		conf: &sts.Conf{},
	}

	if *once {
		if a.conf.Client, _, err = requestClientConf(httpClient, clientID, true); err != nil {
			log.Error(err.Error())
			return
		}
		if a.conf.Client != nil {
			a.runControlledClients(nil)
		}
	} else {
		stopCh := make(chan bool, 2)
		confCh := make(chan *sts.ClientConf)
		go runConfLoader(httpClient, clientID, confCh)
		go a.runStateUpdater(httpClient, clientID, stopCh)
		a.conf.Client = <-confCh
		a.runControlledClients(confCh)
		close(stopCh)
	}

	log.Info("Done")
}

// runControlledClients starts the configured client(s) and, if the input
// channel is not nil, will run in a loop waiting for any config changes to be
// detected or a signal to be received.  If the former, running clients are
// stopped (gracefully) and then restarted using the new configuration.  If the
// input channel is nil, the client(s) will be immediately stopped once they
// have performed a single send iteration.
func (a *app) runControlledClients(confCh <-chan *sts.ClientConf) {
	var err error
	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	runClientCount := 0
	for {
		if a.conf.Client == nil {
			goto wait
		}
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

		if len(a.conf.Client.Sources) == 0 {
			log.Info("No data sources configured")
		} else {
			log.Info("Starting", len(a.conf.Client.Sources), "data source client(s) ...")
			a.startClients()
			runClientCount = len(a.clients)
			log.Info("Started", runClientCount, "data source client(s)")
		}

	wait:
		if confCh == nil {
			a.stopClients(true)
			return
		}
		select {
		case <-signalCh:
			if runClientCount > 0 {
				log.Info("Shutting down", runClientCount, "data source client(s) ...")
				a.stopClients(false)
			}
			return
		case a.conf.Client = <-confCh:
			if runClientCount > 0 {
				log.Info("Stopping", runClientCount, "data source client(s) ...")
				a.stopClients(false)
				runClientCount = 0
			}
			continue
		}
	}
}

func acquireProcLock(clientID string) (lock *single.Single, err error) {
	dir := path.Join(os.TempDir(), "sts")
	if err = os.MkdirAll(dir, 0700); err != nil {
		dir, err = os.UserCacheDir()
		if err != nil {
			return
		}
	}
	lock, err = single.New(
		fmt.Sprintf("sts-client-%s", fileutil.StringMD5(clientID)),
		single.WithLockPath(dir),
	)
	return
}

func releaseProcLock(lock *single.Single) {
	if lock != nil {
		if err := lock.Unlock(); err != nil {
			log.Error(err.Error())
		}
	}
}

func getMachineID(forceCompute bool) (id string, err error) {
	var cacheDir string
	if cacheDir, err = os.UserCacheDir(); err != nil {
		return
	}
	cachePath := filepath.Join(cacheDir, "sts-client-id.txt")
	compute := forceCompute
	if !compute {
		_, err = os.Stat(cachePath)
		compute = os.IsNotExist(err)
		err = nil
	}
	if compute {
		id = uuid.New().String()
		if err = os.MkdirAll(cacheDir, 0700); err != nil {
			return
		}
		if err = os.WriteFile(cachePath, []byte(id), 0644); err != nil {
			return
		}
	}
	var data []byte
	if data, err = os.ReadFile(cachePath); err != nil {
		return
	}
	id = strings.TrimSpace(string(data))
	return
}

func getIpAddr() (addr []string, err error) {
	addr, err = getAddr(false)
	return
}

// getAddr returns the list of MAC or IP addresses for all available interfaces
func getAddr(mac bool) (addr []string, err error) {
	ifas, err := net.Interfaces()
	if err != nil {
		return
	}
	var addrs []net.Addr
	lookup := make(map[string]bool)
	for _, ifa := range ifas {
		if mac {
			a := ifa.HardwareAddr
			// If the second least significant bit of the first octet of the MAC
			// address is 1, then it's a locally administered address.
			// https://en.wikipedia.org/wiki/MAC_address#:~:text=Locally%20administered%20addresses%20are%20distinguished,how%20the%20address%20is%20administered.
			if len(a) > 0 && a[0]&2 != 2 {
				addr = append(addr, a.String())
			}
			continue
		}
		if addrs, err = ifa.Addrs(); err != nil {
			return
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if !ip.IsLoopback() {
				if v4 := ip.To4(); v4 != nil {
					ipv4 := v4.String()
					if _, ok := lookup[ipv4]; ok {
						continue
					}
					addr = append(addr, ipv4)
					lookup[ipv4] = true
				}
			}
		}
	}
	return addr, nil
}

// runConfLoader runs in a loop checking the status of this client and getting
// updated configuration from the server as needed
func runConfLoader(manager sts.ClientManager, clientID string, ch chan<- *sts.ClientConf) {
	var err error
	var disabled bool
	var stopped bool
	var conf *sts.ClientConf
	var newConf *sts.ClientConf
	heartbeat := time.Second * 30
	if StatusInterval != "" {
		if nSec, err := strconv.Atoi(StatusInterval); err == nil {
			heartbeat = time.Second * time.Duration(nSec)
		}
	}
	log.Debug("Heartbeat:", heartbeat)
	for {
		if newConf, disabled, err = requestClientConf(manager, clientID, conf == nil); err != nil {
			log.Error(err.Error())
		}
		if disabled {
			if !stopped {
				log.Info("Client disabled:", clientID)
				stopped = true
				ch <- nil
			}
		} else if newConf != nil {
			log.Info("Client has [updated] configuration:", clientID)
			stopped = false
			conf = newConf
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
	force bool,
) (conf *sts.ClientConf, disabled bool, err error) {
	var name string
	var status sts.ClientStatus
	log.Debug("Getting client status:", clientID)
	if name, err = os.Hostname(); err != nil {
		log.Error(err.Error())
	}
	status, err = manager.GetClientStatus(clientID, name, runtime.GOOS)
	if err != nil {
		return
	}
	switch {
	case status&sts.ClientIsDisabled != 0:
		disabled = true
		return
	case status&sts.ClientIsApproved != 0:
		if force || status&sts.ClientHasUpdatedConfiguration != 0 {
			if conf, err = manager.GetClientConf(clientID); err != nil {
				for _, src := range conf.Sources {
					// This can be anything that doesn't return a matched group because
					// it will invoke the logic that uses the matching tag as the group
					// itself (see main/client.go, grouper & tagger).
					src.GroupBy = regexp.MustCompile(`.`)
				}
				return
			}
			if conf == nil {
				err = fmt.Errorf("an unknown error occurred - check client configuration")
				return
			}
		}
	}
	return
}

// runStateUpdater runs in a loop sending state updates to the server
func (a *app) runStateUpdater(
	manager sts.ClientManager,
	clientID string,
	stopCh <-chan bool,
) {
	interval := time.Second * 60
	if StateInterval != "" {
		if nSec, err := strconv.Atoi(StateInterval); err == nil {
			interval = time.Second * time.Duration(nSec)
		}
	}
	timer := time.NewTimer(time.Second * 1)
	var ok bool
	var stop bool
	for {
		select {
		case stop, ok = <-stopCh:
			if stop || !ok {
				return
			}
			continue
		case <-timer.C:
			ipAddr, err := getIpAddr()
			if err != nil {
				log.Error(err.Error())
			}
			a.isRunningMux.RLock()
			nLogMessages := 10
			if len(a.clients) > 0 {
				nLogMessages = nLogMessages * len(a.clients)
			}
			state := sts.ClientState{
				When:      time.Now().UTC().Format(time.RFC3339),
				Version:   Version,
				GoVersion: runtime.Version(),
				BuildTime: BuildTime,
				IsActive:  a.isRunning,
				IPAddrs:   ipAddr,
				Messages:  log.Recent(nLogMessages),
				Sources:   make(map[string]sts.SourceState),
			}
			if a.isRunning {
				for _, c := range a.clients {
					state.Sources[c.conf.Name] = c.broker.GetState()
				}
			}
			a.isRunningMux.RUnlock()
			if err := manager.SetClientState(clientID, state); err != nil {
				log.Error(err.Error())
			}
			timer.Reset(interval)
		}
	}
}
