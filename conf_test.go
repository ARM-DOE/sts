package sts

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	yaml "gopkg.in/yaml.v2"
)

func TestServerS3Destinations(t *testing.T) {
	for _, format := range []string{"yaml", "json"} {
		for _, withOverrides := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/overrides=%t", format, withOverrides), func(t *testing.T) {
				serverConfig := map[string]interface{}{
					"s3": map[string]interface{}{
						"aws-region": "us-west-2",
						"bucket":     "default-bucket",
						"prefix":     "incoming",
					},
				}
				if withOverrides {
					serverConfig["s3-sources"] = map[string]interface{}{
						"client-b": map[string]interface{}{
							"aws-region": "us-east-1",
							"bucket":     "client-b-bucket",
						},
					}
				}
				input := map[string]interface{}{"IN": serverConfig}
				var encoded []byte
				var err error
				var conf Conf
				if format == "yaml" {
					encoded, err = yaml.Marshal(input)
					if err == nil {
						err = yaml.Unmarshal(encoded, &conf)
					}
				} else {
					encoded, err = json.Marshal(input)
					if err == nil {
						err = json.Unmarshal(encoded, &conf)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if conf.Server == nil || conf.Server.S3 == nil {
					t.Fatal("missing default S3 destination")
				}
				if *conf.Server.S3 != (S3{Region: "us-west-2", Bucket: "default-bucket", Prefix: "incoming"}) {
					t.Fatalf("unexpected default destination: %+v", conf.Server.S3)
				}
				if withOverrides {
					destination := conf.Server.S3Sources["client-b"]
					if destination == nil || *destination != (S3{Region: "us-east-1", Bucket: "client-b-bucket"}) {
						t.Fatalf("unexpected source destination: %+v", destination)
					}
				} else if len(conf.Server.S3Sources) != 0 {
					t.Fatal("legacy config unexpectedly has source overrides")
				}
			})
		}
	}
}

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
    quic:
      max-stream-receive-window: 64MB
      max-connection-receive-window: 268435456
OUT:
  dirs:
    cache: .sts/out
    logs: data/log
    out: data/out
  sources:
    - name: stsout-1
      target:
        http-host: localhost:1992
        quic:
          max-stream-receive-window: 33554432
          max-connection-receive-window: 128MB
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
	if conf.Server.Server.QUIC == nil {
		t.Fatal("missing server QUIC config")
	}
	if conf.Server.Server.QUIC.MaxStreamReceiveWindow != 64*1024*1024 {
		t.Fatalf("unexpected server stream window: %d", conf.Server.Server.QUIC.MaxStreamReceiveWindow)
	}
	if conf.Server.Server.QUIC.MaxConnectionReceiveWindow != 256*1024*1024 {
		t.Fatalf("unexpected server connection window: %d", conf.Server.Server.QUIC.MaxConnectionReceiveWindow)
	}

	if conf.Client == nil || len(conf.Client.Sources) != 1 || conf.Client.Sources[0].Target == nil {
		t.Fatal("missing target config")
	}
	target := conf.Client.Sources[0].Target
	if target.QUIC == nil {
		t.Fatal("missing target QUIC config")
	}
	if target.QUIC.MaxStreamReceiveWindow != 32*1024*1024 {
		t.Fatalf("unexpected target stream window: %d", target.QUIC.MaxStreamReceiveWindow)
	}
	if target.QUIC.MaxConnectionReceiveWindow != 128*1024*1024 {
		t.Fatalf("unexpected target connection window: %d", target.QUIC.MaxConnectionReceiveWindow)
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
        "quic": {
          "max-stream-receive-window": "64MB",
          "max-connection-receive-window": 268435456
        }
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
            "quic": {
              "max-stream-receive-window": 33554432,
              "max-connection-receive-window": "128MB"
            }
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
	if conf.Server.Server.QUIC == nil {
		t.Fatal("missing server QUIC config")
	}
	if conf.Server.Server.QUIC.MaxStreamReceiveWindow != 64*1024*1024 {
		t.Fatalf("unexpected server stream window: %d", conf.Server.Server.QUIC.MaxStreamReceiveWindow)
	}
	if conf.Server.Server.QUIC.MaxConnectionReceiveWindow != 256*1024*1024 {
		t.Fatalf("unexpected server connection window: %d", conf.Server.Server.QUIC.MaxConnectionReceiveWindow)
	}

	if conf.Client == nil || len(conf.Client.Sources) != 1 || conf.Client.Sources[0].Target == nil {
		t.Fatal("missing target config")
	}
	target := conf.Client.Sources[0].Target
	if target.QUIC == nil {
		t.Fatal("missing target QUIC config")
	}
	if target.QUIC.MaxStreamReceiveWindow != 32*1024*1024 {
		t.Fatalf("unexpected target stream window: %d", target.QUIC.MaxStreamReceiveWindow)
	}
	if target.QUIC.MaxConnectionReceiveWindow != 128*1024*1024 {
		t.Fatalf("unexpected target connection window: %d", target.QUIC.MaxConnectionReceiveWindow)
	}
}

func TestYAMLKCPHTTPConfiguration(t *testing.T) {
	confEncoded := `
IN:
  dirs:
    stage: data/stage
    final: data/in
    logs: data/log
  server:
    http-host: localhost
    http-port: 1992
    kcp-enabled: true
    kcp:
      host: 0.0.0.0
      send-window: 8192
      receive-window: 16384
      socket-read-buffer: 32MB
      socket-write-buffer: 64MB
      idle-timeout: 90s
OUT:
  dirs:
    cache: .sts/out
    logs: data/log
    out: data/out
  sources:
    - name: stsout-1
      target:
        http-host: localhost:1992
        protocol: kcp-http
        kcp:
          host: receiver.example.org
          data-shards: 10
          parity-shards: 3
          disable-congestion-control: true
  `

	var conf Conf
	if err := yaml.Unmarshal([]byte(confEncoded), &conf); err != nil {
		t.Fatal(err)
	}

	serverKCP := conf.Server.Server.KCP
	if !conf.Server.Server.EnableKCP || serverKCP == nil {
		t.Fatal("missing server KCP configuration")
	}
	if serverKCP.Host != "0.0.0.0" || serverKCP.Port != 1992 {
		t.Fatalf("unexpected server KCP address: %s:%d", serverKCP.Host, serverKCP.Port)
	}
	if serverKCP.Tuning != KCPTuningBalanced {
		t.Fatalf("unexpected server KCP tuning profile: %q", serverKCP.Tuning)
	}
	if serverKCP.MTU != 1350 || serverKCP.DataShards != 20 || serverKCP.ParityShards != 2 {
		t.Fatalf("unexpected server KCP defaults: %+v", serverKCP)
	}
	if serverKCP.SendWindow != 8192 || serverKCP.ReceiveWindow != 16384 {
		t.Fatalf("unexpected server KCP windows: %+v", serverKCP)
	}
	if serverKCP.SocketReadBuffer != 32*1024*1024 || serverKCP.SocketWriteBuffer != 64*1024*1024 {
		t.Fatalf("unexpected server KCP socket buffers: %+v", serverKCP)
	}
	if serverKCP.IdleTimeout != 90*time.Second {
		t.Fatalf("unexpected server KCP idle timeout: %s", serverKCP.IdleTimeout)
	}

	target := conf.Client.Sources[0].Target
	if target.Protocol != "kcp-http" || target.KCP == nil {
		t.Fatalf("unexpected target protocol configuration: %+v", target)
	}
	if target.KCP.Host != "receiver.example.org" || target.KCP.Port != 1992 {
		t.Fatalf("unexpected target KCP address: %+v", target.KCP)
	}
	if target.KCP.DataShards != 10 || target.KCP.ParityShards != 3 {
		t.Fatalf("unexpected target FEC configuration: %+v", target.KCP)
	}
	if !target.KCP.NoDelay || target.KCP.Interval != 20*time.Millisecond || target.KCP.FastResend != 2 {
		t.Fatalf("unexpected target KCP timing defaults: %+v", target.KCP)
	}
	if !target.KCP.DisableCongestionControl {
		t.Fatal("expected congestion control to be disabled")
	}
}

func TestYAMLKCPFECDisabledAndInherited(t *testing.T) {
	confEncoded := `
OUT:
  sources:
    - name: first
      target:
        http-host: receiver.example.org:1992
        protocol: kcp-http
        kcp:
          port: 1993
          fec-enabled: false
          duplicate-packets: 1
    - name: second
      target:
        name: inherited-target
  `
	var conf Conf
	if err := yaml.Unmarshal([]byte(confEncoded), &conf); err != nil {
		t.Fatal(err)
	}
	first := conf.Client.Sources[0].Target
	if first.KCP == nil || first.KCP.FECEnabled || first.KCP.DataShards != 0 || first.KCP.ParityShards != 0 || first.KCP.DuplicatePackets != 1 {
		t.Fatalf("FEC was not disabled: %+v", first.KCP)
	}
	second := conf.Client.Sources[1].Target
	if second.Protocol != "kcp-http" || second.KCP == nil || second.KCP.Port != 1993 {
		t.Fatalf("KCP target configuration was not inherited: %+v", second)
	}
}

func TestYAMLKCPExplicitPortOverridesHTTPPort(t *testing.T) {
	confEncoded := `
IN:
  server:
    http-port: 1992
    kcp-enabled: true
    kcp:
      port: 1993
`
	var conf Conf
	if err := yaml.Unmarshal([]byte(confEncoded), &conf); err != nil {
		t.Fatal(err)
	}
	if conf.Server.Server.KCP.Port != 1993 {
		t.Fatalf("explicit KCP port was replaced: %d", conf.Server.Server.KCP.Port)
	}
}

func TestKCPConfValidate(t *testing.T) {
	valid := KCPConf{
		Port:              1993,
		Tuning:            KCPTuningBalanced,
		MTU:               1350,
		SendWindow:        4096,
		ReceiveWindow:     4096,
		FECEnabled:        true,
		DataShards:        20,
		ParityShards:      2,
		Interval:          20 * time.Millisecond,
		SocketReadBuffer:  1024,
		SocketWriteBuffer: 1024,
		IdleTimeout:       time.Minute,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*KCPConf)
	}{
		{name: "port", mutate: func(c *KCPConf) { c.Port = 0 }},
		{name: "tuning", mutate: func(c *KCPConf) { c.Tuning = "auto" }},
		{name: "mtu", mutate: func(c *KCPConf) { c.MTU = 1501 }},
		{name: "window", mutate: func(c *KCPConf) { c.SendWindow = 65536 }},
		{name: "fec pair", mutate: func(c *KCPConf) { c.ParityShards = 0 }},
		{name: "fec total", mutate: func(c *KCPConf) { c.DataShards, c.ParityShards = 250, 10 }},
		{name: "duplicate packets", mutate: func(c *KCPConf) { c.DuplicatePackets = 11 }},
		{name: "duplicate packets with fec", mutate: func(c *KCPConf) { c.DuplicatePackets = 1 }},
		{name: "interval", mutate: func(c *KCPConf) { c.Interval = time.Millisecond }},
		{name: "idle timeout", mutate: func(c *KCPConf) { c.IdleTimeout = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := valid
			test.mutate(&conf)
			if err := conf.Validate(); err == nil {
				t.Fatalf("invalid configuration accepted: %+v", conf)
			}
		})
	}
}
