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
