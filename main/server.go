package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/control"
	"code.arm.gov/dataflow/sts/dispatch"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/payload"
	"code.arm.gov/dataflow/sts/stage"
	"github.com/aws/aws-sdk-go/aws"
)

func strToIndex(needle string, haystack []string) int {
	for i, v := range haystack {
		if v == needle {
			return i
		}
	}
	return -1
}

type serverApp struct {
	debug  bool
	conf   *sts.ServerConf
	server *http.Server
}

func (a *serverApp) standardValidator(source, key string) bool {
	if len(a.conf.Sources) > 0 {
		if matched, err := regexp.MatchString(`^[a-z0-9\.\-/]+$`, source); err != nil || !matched {
			return false
		}
		if strToIndex(source, a.conf.Sources) < 0 {
			return false
		}
	}
	if len(a.conf.Keys) > 0 && strToIndex(key, a.conf.Keys) < 0 {
		return false
	}
	return true
}

func (a *serverApp) init() (err error) {
	conf := a.conf
	if conf.Server.Port == 0 {
		err = fmt.Errorf("HTTP port not specified")
		return
	}
	dirs := conf.Dirs
	log.Init(dirs.LogMsg, a.debug)
	var dispatcher *dispatch.Queue
	if conf.Queue != nil {
		dispatcher, err = dispatch.NewQueue(
			&aws.Config{Region: aws.String(conf.Queue.Region)},
			conf.Queue.Name,
		)
		if err != nil {
			return
		}
	}
	newStage := func(source string) sts.GateKeeper {
		return stage.New(
			source,
			filepath.Join(dirs.Stage, source),
			filepath.Join(dirs.Final, source),
			log.NewReceive(filepath.Join(dirs.LogIn, source)),
			dispatcher,
		)
	}
	nodes, err := ioutil.ReadDir(dirs.Stage)
	if err != nil {
		return
	}
	var stager sts.GateKeeper
	var stagers map[string]sts.GateKeeper
	stagers = make(map[string]sts.GateKeeper)
	for _, node := range nodes {
		if node.IsDir() {
			stager = newStage(node.Name())
			if err = stager.Recover(); err != nil {
				return
			}
			stagers[node.Name()] = stager
		}
	}
	a.server = &http.Server{
		ServeDir:          dirs.Serve,
		Host:              conf.Server.Host,
		Port:              conf.Server.Port,
		PathPrefix:        conf.Server.PathPrefix,
		Compression:       conf.Server.Compression,
		DecoderFactory:    payload.NewDecoder,
		IsValid:           a.standardValidator,
		GateKeepers:       stagers,
		GateKeeperFactory: newStage,
	}
	if conf.Server.TLSCertPath != "" && conf.Server.TLSKeyPath != "" {
		if a.server.TLS, err = http.LoadTLSConf(
			conf.Server.TLSCertPath,
			conf.Server.TLSKeyPath, ""); err != nil {
			return
		}
	}
	if conf.SourceControl != nil {
		db := control.NewPostgres(
			conf.SourceControl.Port,
			conf.SourceControl.Host,
			conf.SourceControl.Name,
			conf.SourceControl.User,
			conf.SourceControl.Pass,
			conf.SourceControl.ClientsTable,
			conf.SourceControl.DatasetsTable,
			decodeClientID,
		)
		a.server.ClientManager = db
		a.server.IsValid = db.IsValid
	}
	return
}
