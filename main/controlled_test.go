package main

import (
	"testing"
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
// func TestConf(t *testing.T) {
// 	var err error

// 	rootDir := os.Getenv("STS_HOME")
// 	if rootDir == "" {
// 		rootDir, err = os.UserCacheDir()
// 		if err != nil {
// 			panic(err)
// 		}
// 		rootDir = filepath.Join(rootDir, "sts")
// 	}

// 	log.InitExternal(newLogger(true))

// 	log.Info("Cache Directory:", rootDir)

// 	bytes := []byte(`{
//         "name": "dap",
//         "key": "...",
//         "http-host": "a2e.energy.gov:443",
//         "http-path-prefix": "/_upload",
//         "http-tls-cert": "",
//         "http-tls-cert-encoded": ""
//     }`)
// 	serverConf := sts.TargetConf{}
// 	if err := json.Unmarshal(bytes, &serverConf); err != nil {
// 		panic(err)
// 	}

// 	tls, err := getTLSConf(&serverConf)
// 	if err != nil {
// 		panic(err)
// 	}
// 	host, port, err := serverConf.ParseHost()
// 	if err != nil {
// 		panic(err)
// 	}
// 	httpClient := &http.Client{
// 		TargetHost:   host,
// 		TargetPort:   port,
// 		TargetPrefix: serverConf.PathPrefix,
// 		TLS:          tls,
// 	}

// 	a := &app{
// 		loop: true,
// 		mode: modeSend,
// 		root: rootDir,
// 		conf: &sts.Conf{},
// 	}

// 	clientID := "<key>:<id>"

// 	if a.conf.Client, err = requestClientConf(httpClient, clientID, true); err != nil {
// 		t.Fatal(err.Error())
// 		return
// 	}

// 	for _, source := range a.conf.Client.Sources {
// 		c := &clientApp{
// 			conf:         source,
// 			dirCache:     a.conf.Client.Dirs.Cache,
// 			dirOutFollow: a.conf.Client.Dirs.OutFollow,
// 		}
// 		if err = c.init(); err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	t.Fatal(spew.Sdump(a.conf.Client))
// }
