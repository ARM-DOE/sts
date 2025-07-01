package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/control"
	"github.com/arm-doe/sts/dispatch"
	"github.com/arm-doe/sts/export"
	"github.com/arm-doe/sts/http"
	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/payload"
	"github.com/arm-doe/sts/stage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
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
	log.Init(dirs.LogMsg, a.debug, nil, nil)

	var dispatcher sts.Dispatcher
	if conf.Queue != nil {
		var cfg aws.Config
		cfg, err = config.LoadDefaultConfig(context.Background(),
			func(o *config.LoadOptions) error {
				if conf.Queue.Region != "" {
					o.Region = conf.Queue.Region
				}
				return nil
			})
		if err != nil {
			return
		}
		dispatcher, err = dispatch.NewSQS(
			cfg,
			conf.Queue.Name,
		)
		if err != nil {
			return
		}
	}

	var exporter sts.Exporter
	if conf.S3 != nil {
		var cfg aws.Config
		cfg, err = config.LoadDefaultConfig(context.Background(),
			func(o *config.LoadOptions) error {
				if conf.S3.Region != "" {
					o.Region = conf.S3.Region
				}
				return nil
			})
		if err != nil {
			return
		}
		exporter, err = export.NewS3Upload(
			cfg,
			conf.S3.Bucket,
			conf.S3.Prefix,
		)
		if err != nil {
			return
		}
	}

	pathSep := string(os.PathSeparator)
	pathSepRepl := "--"

	newStage := func(source string) sts.GateKeeper {
		sourcePathReady := strings.ReplaceAll(source, pathSep, pathSepRepl)
		return stage.New(
			source,
			filepath.Join(dirs.Stage, sourcePathReady),
			filepath.Join(dirs.Final, sourcePathReady),
			log.NewFileIO(
				filepath.Join(dirs.LogIn, sourcePathReady),
				nil, nil, !a.conf.PermitLogBuf),
			dispatcher,
			exporter,
		)
	}

	var stager sts.GateKeeper
	stagers := make(map[string]sts.GateKeeper)

	nodes, err := os.ReadDir(dirs.Stage)
	if err != nil {
		return
	}
	for _, node := range nodes {
		if node.IsDir() {
			name := strings.ReplaceAll(node.Name(), pathSepRepl, pathSep)
			stager = newStage(name)
			stagers[name] = stager
			go stager.Recover()
		}
	}

	hsts := &conf.Server.HSTS
	if !conf.Server.HSTSEnabled {
		hsts = nil
	}

	a.server = &http.Server{
		ServeDir:                 dirs.Serve,
		Host:                     conf.Server.Host,
		Port:                     conf.Server.Port,
		PathPrefix:               conf.Server.PathPrefix,
		Compression:              conf.Server.Compression,
		DecoderFactory:           payload.NewDecoder,
		IsValid:                  a.standardValidator,
		GateKeepers:              stagers,
		GateKeeperFactory:        newStage,
		ChanceOfSimulatedFailure: conf.Server.ChanceOfSimulatedFailure,
		HSTS:                     hsts,
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
