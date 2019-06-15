package sts

import (
	"encoding/json"
	"strings"
	"testing"

	yaml "gopkg.in/yaml.v2"
)

func TestJSON(t *testing.T) {
	confEncoded := []string{`
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
        }
    }
    `, `
    {
      "tags": [
        {
          "pattern": "DEFAULT",
          "delete": "true"
        }
      ]
    }
  `}
	var err error
	var conf SourceConf
	for _, j := range confEncoded {
		err = json.Unmarshal([]byte(j), &conf)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestYAML(t *testing.T) {
	confEncoded := []string{`
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
            key           : 'uB7Te#H>"a%5^6p['
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
IN:
    sources:
        - stsout-1
        - stsout-2
    keys:
            - 'uB7Te#H>"a%5^6p['
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
    `}
	var err error
	var conf Conf
	for _, y := range confEncoded {
		err = yaml.Unmarshal(
			[]byte(
				strings.Trim(
					strings.ReplaceAll(y, "\t", " "),
					"\n ",
				),
			),
			&conf,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
}
