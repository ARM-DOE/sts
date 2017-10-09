package main

import (
	"io/ioutil"
	"path/filepath"
	"regexp"
	"time"

	"code.arm.gov/dataflow/sts/queue"
	"code.arm.gov/dataflow/sts/reflectutil"

	"github.com/alecthomas/units"
	"gopkg.in/yaml.v2"
)

const (
	defaultTag = "DEFAULT"
	methodHTTP = "http"
)

type conf struct {
	Client *clientConf `yaml:"OUT"`
	Server *serverConf `yaml:"IN"`
}

// clientConf is the struct for housing all outgoing configuration.
type clientConf struct {
	Dirs    *clientDirs
	Sources []*sourceConf
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
// This is where we implement the feature of propagating options.
func (conf *clientConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Dirs    *clientDirs   `yaml:"dirs"`
		Sources []*sourceConf `yaml:"sources"`
	}
	if err = unmarshal(&aux); err != nil {
		return
	}
	conf.Dirs = aux.Dirs
	conf.Sources = aux.Sources
	if len(conf.Sources) == 0 {
		return
	}
	var src *sourceConf
	var tgt *sourceConf
	for i := 0; i < len(conf.Sources); i++ {
		if src != nil {
			tgt = conf.Sources[i]
			reflectutil.CopyStruct(tgt, src)
			if src.Target != nil && tgt.Target != nil {
				reflectutil.CopyStruct(tgt.Target, src.Target)
			}
		}
		src = conf.Sources[i]
		if len(src.Tags) > 1 {
			tdef := src.Tags[0]
			for j := 1; j < len(src.Tags); j++ {
				reflectutil.CopyStruct(src.Tags[j], tdef)
			}
		}
	}
	return
}

// clientDirs is the struct for managing the outgoing directory configuration
// items.
type clientDirs struct {
	Out       string `yaml:"out"`
	OutFollow bool   `yaml:"out-follow"`
	Logs      string `yaml:"logs"`
	LogsOut   string `yaml:"logs-out"`
	LogsMsg   string `yaml:"logs-flow"`
	Cache     string `yaml:"cache"`
}

// sourceConf is the struct for managing the configuration of an outgoing
// client source.
type sourceConf struct {
	Name         string
	OutDir       string
	Threads      int
	CacheAge     time.Duration
	MinAge       time.Duration
	MaxAge       time.Duration
	ScanDelay    time.Duration
	Timeout      time.Duration
	BinSize      units.Base2Bytes
	Compression  int
	StatInterval time.Duration
	PollDelay    time.Duration
	PollInterval time.Duration
	PollAttempts int
	Target       *targetConf
	GroupBy      *regexp.Regexp
	Include      []*regexp.Regexp
	Ignore       []*regexp.Regexp
	Tags         []*tagConf
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s).
// https://godoc.org/gopkg.in/yaml.v2
func (ss *sourceConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Name         string        `yaml:"name"`
		OutDir       string        `yaml:"out-dir"`
		Threads      int           `yaml:"threads"`
		CacheAge     time.Duration `yaml:"cache-age"`
		MinAge       time.Duration `yaml:"min-age"`
		MaxAge       time.Duration `yaml:"max-age"`
		ScanDelay    time.Duration `yaml:"scan-delay"`
		Timeout      time.Duration `yaml:"timeout"`
		BinSize      string        `yaml:"bin-size"`
		Compression  int           `yaml:"compress"`
		StatInterval time.Duration `yaml:"stat-interval"`
		PollDelay    time.Duration `yaml:"poll-delay"`
		PollInterval time.Duration `yaml:"poll-interval"`
		PollAttempts int           `yaml:"poll-attempts"`
		Target       *targetConf   `yaml:"target"`
		GroupBy      string        `yaml:"group-by"`
		Include      []string      `yaml:"include"`
		Ignore       []string      `yaml:"ignore"`
		Tags         []*tagConf    `yaml:"tags"`
	}
	if err = unmarshal(&aux); err != nil {
		return
	}
	ss.Name = aux.Name
	ss.OutDir = aux.OutDir
	ss.Threads = aux.Threads
	ss.CacheAge = aux.CacheAge
	ss.MinAge = aux.MinAge
	ss.MaxAge = aux.MaxAge
	ss.ScanDelay = aux.ScanDelay
	ss.Timeout = aux.Timeout
	if aux.BinSize != "" {
		if ss.BinSize, err = units.ParseBase2Bytes(aux.BinSize); err != nil {
			return
		}
	}
	ss.Compression = aux.Compression
	ss.StatInterval = aux.StatInterval
	ss.PollDelay = aux.PollDelay
	ss.PollInterval = aux.PollInterval
	ss.PollAttempts = aux.PollAttempts
	ss.Target = aux.Target
	if aux.GroupBy != "" {
		if ss.GroupBy, err = regexp.Compile(aux.GroupBy); err != nil {
			return
		}
	}
	var patterns []*regexp.Regexp
	for _, s := range append(aux.Include, aux.Ignore...) {
		var p *regexp.Regexp
		if p, err = regexp.Compile(s); err != nil {
			return
		}
		patterns = append(patterns, p)
	}
	ss.Include = patterns[0:len(aux.Include)]
	ss.Ignore = patterns[len(aux.Include):len(patterns)]
	ss.Tags = aux.Tags
	return
}

// tagConf is the struct for managing configuration options for "tags" (groups)
// of files.
type tagConf struct {
	Pattern   *regexp.Regexp
	Priority  int
	Method    string
	Order     string
	Delete    bool
	LastDelay time.Duration
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
func (t *tagConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Pattern   string        `yaml:"pattern"`
		Priority  int           `yaml:"priority"`
		Method    string        `yaml:"method"`
		Order     string        `yaml:"order"`
		Delete    bool          `yaml:"delete"`
		LastDelay time.Duration `yaml:"last-delay"`
	}
	if err = unmarshal(&aux); err != nil {
		return
	}
	if aux.Pattern != defaultTag {
		if t.Pattern, err = regexp.Compile(aux.Pattern); err != nil {
			return
		}
	} else {
		if aux.Order == "" {
			aux.Order = queue.ByFIFO
		}
		if aux.Method == "" {
			aux.Method = methodHTTP
		}
	}
	if aux.Method == "" {
		aux.Method = methodHTTP
	}
	t.Priority = aux.Priority
	t.Method = aux.Method
	t.Order = aux.Order
	t.Delete = aux.Delete
	t.LastDelay = aux.LastDelay
	return
}

// targetConf houses the configuration for the target host for a given source
type targetConf struct {
	Name        string `yaml:"name"`
	Key         string `yaml:"key"`
	Host        string `yaml:"http-host"`
	TLSCertPath string `yaml:"http-tls-cert"`
}

// serverConf is the struct for housing all incoming configuration
type serverConf struct {
	Sources []string    `yaml:"sources"`
	Keys    []string    `yaml:"keys"`
	Dirs    *serverDirs `yaml:"dirs"`
	Server  *httpServer `yaml:"server"`
}

// serverDirs is the struct for managing the incoming directory configuration
// items.
type serverDirs struct {
	Logs    string `yaml:"logs"`
	LogsIn  string `yaml:"logs-in"`
	LogsMsg string `yaml:"logs-flow"`
	Stage   string `yaml:"stage"`
	Final   string `yaml:"final"`
	Serve   string `yaml:"serve"`
}

// httpServer is the struct for managing the incoming HTTP host
type httpServer struct {
	Host        string `yaml:"http-host"`
	Port        int    `yaml:"http-port"`
	TLSCertPath string `yaml:"http-tls-cert"`
	TLSKeyPath  string `yaml:"http-tls-key"`
	Compression int    `yaml:"compress"`
}

// newConf returns a parsed Conf reference based on the provided conf file path
func newConf(path string) (*conf, error) {
	var err error
	conf := &conf{}
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return conf, err
		}
	}
	fh, err := ioutil.ReadFile(path)
	if err != nil {
		panic("Configuration file not found: " + path)
	}
	err = yaml.Unmarshal(fh, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}
