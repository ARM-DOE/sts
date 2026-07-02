package sts

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"

	yaml "gopkg.in/yaml.v2"
)

func TestJSON(t *testing.T) {
	confEncoded := `
  {
    "OUT": {
      "dirs": {
        "cache": ".sts/out",
        "logs": "data/log",
        "out": "data/out",
        "out-follow": true
      },
      "sources": [
        {
          "name": "stsout-1",
          "threads": 4,
          "scan-delay": "10s",
          "cache-age": "5m",
          "min-age": "5s",
          "max-age": "0s",
          "bin-size": "20MB",
          "compress": 0,
          "stat-payload": "true",
          "poll-delay": "2s",
          "poll-interval": "5s",
          "poll-attempts": 1,
          "target": {
            "name": "aname",
            "key": "akey",
            "http-host": "host:port",
            "http-tls-cert-encoded": "blahblahblah"
          },
          "tags": [
            {
              "pattern": "DEFAULT",
              "delete": "true"
            }
          ]
        }
      ]
    }
  }
  `
	var conf Conf
	if err := json.Unmarshal([]byte(confEncoded), &conf); err != nil {
		t.Fatal(err)
	}
	// spew.Dump(conf)
}

func TestYAML(t *testing.T) {
	confEncoded := `
IN:
  sources:
    - stsout-1
    - stsout-2
  keys:
    - 'testing123'
  dirs:
    stage : data/stage
    final : data/in
    logs  : data/log
  server:
    http-host     : localhost
    http-port     : 1992
    http-tls-cert : ../../sts.server.crt
    http-tls-key  : ../../sts.server.key
    compress      : 4
OUT:
  dirs:
    cache      : .sts/out
    logs       : data/log
    out        : data/out
    out-follow : true
  sources:
    - name          : stsout-1
      threads       : 3
      scan-delay    : 5s
      min-age       : 0s
      max-age       : 0s
      timeout       : 5m
      bin-size      : 20MB
      compress      : 0
      stat-payload  : true
      poll-delay    : 2s
      poll-interval : 5s
      poll-attempts : 1
      target:
        name          : stsin-1
        key           : 'testing123'
        http-host     : localhost:1992
        http-tls-cert : ../../sts.server.crt
      tags:
        - pattern   : DEFAULT
          priority  : 0
          order     : fifo
          delete    : true
          method    : http
        - pattern   : ^info/
          priority  : 3
        - pattern   : ^comlogs/
          priority  : 2
        - pattern   : ^collection/
          priority  : 1
      rename:
        - from: '/(?P<prefix>[^/]+)\.(?P<YMD>\d{8})\.(?P<HMS>\d{6})\.(?P<rest>[^/]+)$'
          to: '{{.prefix}}/{{.YMD}}-{{.HMS}}.{{.rest}}'
    - name: stsout-2
      target:
        name: stsin-2
  `
	var conf Conf
	encoded := strings.Trim(
		strings.ReplaceAll(confEncoded, "\t", "  "),
		"\n ",
	)
	if err := yaml.Unmarshal([]byte(encoded), &conf); err != nil {
		t.Fatal(err)
	}
	if conf.Client.Dirs == nil {
		t.Fatal(spew.Sdump(conf))
	}
	if len(conf.Client.Sources) != 2 {
		t.Fatal(spew.Sdump(conf))
	}
	// spew.Dump(conf)
}

func TestYAMLQUICWindowFriendlySizes(t *testing.T) {
	confEncoded := `
IN:
  dirs:
    stage : data/stage
    final : data/in
    logs  : data/log
  server:
    http-host: localhost
    http-port: 1992
    quic-max-stream-receive-window: 64MB
    quic-max-connection-receive-window: 268435456
OUT:
  dirs:
    cache: .sts/out
    logs: data/log
    out: data/out
  sources:
    - name: stsout-1
      target:
        http-host: localhost:1992
        quic-max-stream-receive-window: 33554432
        quic-max-connection-receive-window: 128MB
  `
	var conf Conf
	encoded := strings.Trim(
		strings.ReplaceAll(confEncoded, "\t", "  "),
		"\n ",
	)
	if err := yaml.Unmarshal([]byte(encoded), &conf); err != nil {
		t.Fatal(err)
	}

	if conf.Server == nil || conf.Server.Server == nil {
		t.Fatal("missing server config")
	}
	if conf.Server.Server.QUICMaxStreamReceiveWindow != 64*1024*1024 {
		t.Fatalf("unexpected server stream window: %d", conf.Server.Server.QUICMaxStreamReceiveWindow)
	}
	if conf.Server.Server.QUICMaxConnectionReceiveWindow != 256*1024*1024 {
		t.Fatalf("unexpected server connection window: %d", conf.Server.Server.QUICMaxConnectionReceiveWindow)
	}

	if conf.Client == nil || len(conf.Client.Sources) != 1 || conf.Client.Sources[0].Target == nil {
		t.Fatal("missing target config")
	}
	target := conf.Client.Sources[0].Target
	if target.QUICMaxStreamReceiveWindow != 32*1024*1024 {
		t.Fatalf("unexpected target stream window: %d", target.QUICMaxStreamReceiveWindow)
	}
	if target.QUICMaxConnectionReceiveWindow != 128*1024*1024 {
		t.Fatalf("unexpected target connection window: %d", target.QUICMaxConnectionReceiveWindow)
	}
}

func TestJSONQUICWindowFriendlySizes(t *testing.T) {
	confEncoded := `
  {
    "IN": {
      "dirs": {
        "stage": "data/stage",
        "final": "data/in",
        "logs": "data/log"
      },
      "server": {
        "http-host": "localhost",
        "http-port": 1992,
        "quic-max-stream-receive-window": "64MB",
        "quic-max-connection-receive-window": 268435456
      }
    },
    "OUT": {
      "dirs": {
        "cache": ".sts/out",
        "logs": "data/log",
        "out": "data/out"
      },
      "sources": [
        {
          "name": "stsout-1",
          "target": {
            "http-host": "localhost:1992",
            "quic-max-stream-receive-window": 33554432,
            "quic-max-connection-receive-window": "128MB"
          }
        }
      ]
    }
  }
  `
	var conf Conf
	if err := json.Unmarshal([]byte(confEncoded), &conf); err != nil {
		t.Fatal(err)
	}

	if conf.Server == nil || conf.Server.Server == nil {
		t.Fatal("missing server config")
	}
	if conf.Server.Server.QUICMaxStreamReceiveWindow != 64*1024*1024 {
		t.Fatalf("unexpected server stream window: %d", conf.Server.Server.QUICMaxStreamReceiveWindow)
	}
	if conf.Server.Server.QUICMaxConnectionReceiveWindow != 256*1024*1024 {
		t.Fatalf("unexpected server connection window: %d", conf.Server.Server.QUICMaxConnectionReceiveWindow)
	}

	if conf.Client == nil || len(conf.Client.Sources) != 1 || conf.Client.Sources[0].Target == nil {
		t.Fatal("missing target config")
	}
	target := conf.Client.Sources[0].Target
	if target.QUICMaxStreamReceiveWindow != 32*1024*1024 {
		t.Fatalf("unexpected target stream window: %d", target.QUICMaxStreamReceiveWindow)
	}
	if target.QUICMaxConnectionReceiveWindow != 128*1024*1024 {
		t.Fatalf("unexpected target connection window: %d", target.QUICMaxConnectionReceiveWindow)
	}
}
