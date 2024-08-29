package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
)

const (
	ClientID = ""
)

func TestMachineID(t *testing.T) {
	id0, err := getMachineID(true)
	if err != nil {
		t.Fatal(err)
	}
	id1, err := getMachineID(false)
	if err != nil {
		t.Fatal(err)
	}
	if id0 != id1 {
		t.Fatalf("%s != %s", id0, id1)
	}
}

// Useful as a one-off to debug real deployed controlled clients
func TestConf(t *testing.T) {
	if ClientID == "" {
		t.Skip("ClientID not set")
	}
	var err error

	rootDir := os.Getenv("STS_HOME")
	if rootDir == "" {
		rootDir, err = os.UserCacheDir()
		if err != nil {
			panic(err)
		}
		rootDir = filepath.Join(rootDir, "sts")
	}

	log.InitExternal(newLogger(true))

	log.Info("Cache Directory:", rootDir)

	bytes := []byte(fmt.Sprintf(`{
        "name": "dap",
        "key": "%s",
        "http-host": "a2e.energy.gov:443",
        "http-path-prefix": "/_upload",
        "http-tls-cert": "",
        "http-tls-cert-encoded": ""
    }`, ClientID))
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

	a := &app{
		// loop: true,
		// mode: modeSend,
		// root: rootDir,
		conf: &sts.Conf{},
	}

	if a.conf.Client, _, err = requestClientConf(httpClient, ClientID, true); err != nil {
		t.Fatal(err.Error())
		return
	}

	for _, source := range a.conf.Client.Sources {
		c := &clientApp{
			conf:         source,
			dirCache:     a.conf.Client.Dirs.Cache,
			dirOutFollow: a.conf.Client.Dirs.OutFollow,
		}
		if err = c.init(); err != nil {
			t.Fatal(err)
		}
	}

	// t.Fatal(spew.Sdump(a.conf.Client))
}
