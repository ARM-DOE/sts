package main

import (
	"fmt"
	"path/filepath"

	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/payload"
	"code.arm.gov/dataflow/sts/stage"
)

type serverApp struct {
	root string
	conf *serverConf
	http *http.Server
}

func (s *serverApp) init() (err error) {
	stageDir := s.conf.Dirs.Stage
	if stageDir == "" {
		stageDir = "stage"
	}
	finalDir := s.conf.Dirs.Final
	if finalDir == "" {
		finalDir = "final"
	}
	if stageDir, err = fileutil.InitPath(s.root, stageDir, true); err != nil {
		return
	}
	if finalDir, err = fileutil.InitPath(s.root, finalDir, true); err != nil {
		return
	}
	logDir := filepath.Join(s.conf.Dirs.Logs, s.conf.Dirs.LogsIn)
	if logDir, err = fileutil.InitPath(s.root, logDir, true); err != nil {
		return
	}
	stager := stage.New(stageDir, finalDir, log.NewReceive(logDir))
	if err = stager.Recover(); err != nil {
		return
	}
	s.http = &http.Server{
		ServeDir:       s.conf.Dirs.Serve,
		Host:           s.conf.Server.Host,
		Port:           s.conf.Server.Port,
		Sources:        s.conf.Sources,
		Keys:           s.conf.Keys,
		Compression:    s.conf.Server.Compression,
		DecoderFactory: payload.NewDecoder,
		GateKeeper:     stager,
	}
	if s.http.ServeDir != "" {
		if s.http.ServeDir, err = fileutil.InitPath(s.root, s.http.ServeDir, true); err != nil {
			return
		}
	}
	if s.http.Port == 0 {
		err = fmt.Errorf("HTTP server port not specified")
		return
	}
	if s.conf.Server.TLSCertPath != "" && s.conf.Server.TLSKeyPath != "" {
		var certPath, keyPath string
		if certPath, err = fileutil.InitPath(s.root, s.conf.Server.TLSCertPath, false); err != nil {
			return
		}
		if keyPath, err = fileutil.InitPath(s.root, s.conf.Server.TLSKeyPath, false); err != nil {
			return
		}
		if s.http.TLS, err = http.GetTLSConf(certPath, keyPath, ""); err != nil {
			return
		}
	}
	return
}
