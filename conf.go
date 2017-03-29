package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"regexp"
	"time"

	"github.com/alecthomas/units"
	"gopkg.in/yaml.v2"
)

const (

	// DefaultTag is the name of the "default" tag.
	DefaultTag = "DEFAULT"

	// OrderNone indicates order is not important for outgoing files.
	OrderNone = "none"

	// OrderFIFO indicates a first in, first out ordering for outgoing files.
	OrderFIFO = "fifo"

	// OrderLIFO indicates a last in, first out ordering for outgoing files.
	OrderLIFO = "lifo"

	// MethodHTTP indicates an HTTP outgoing method.
	MethodHTTP = "http"

	// MethodDisk indicates a disk transfer method.
	MethodDisk = "disk"
)

// Conf is the outermost struct for holding STS configuration.
type Conf struct {
	Out *OutConf `yaml:"OUT"`
	In  *InConf  `yaml:"IN"`

	path string
}

// OutConf is the struct for housing all outgoing configuration.
type OutConf struct {
	Dirs    *OutDirs     `yaml:"dirs"`
	Sources []*OutSource `yaml:"sources"`
}

// OutDirs is the struct for managing the outgoing directory configuration items.
type OutDirs struct {
	Out       string `yaml:"out"`
	OutFollow bool   `yaml:"out-follow"`
	Logs      string `yaml:"logs"`
	LogsOut   string `yaml:"logs-out"`
	LogsMsg   string `yaml:"logs-flow"`
	Disk      string `yaml:"disk"`
	Cache     string `yaml:"cache"`
}

// OutSource is the struct for managing the configuration of an outgoing source.
type OutSource struct {
	Name         string
	OutDir       string
	Threads      int
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
	Target       *OutTarget
	GroupBy      *regexp.Regexp
	Include      []*regexp.Regexp
	Ignore       []*regexp.Regexp
	Tags         []*Tag
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s).
// https://godoc.org/gopkg.in/yaml.v2
func (ss *OutSource) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Name         string        `yaml:"name"`
		OutDir       string        `yaml:"out-dir"`
		Threads      int           `yaml:"threads"`
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
		Target       *OutTarget    `yaml:"target"`
		GroupBy      string        `yaml:"group-by"`
		Include      []string      `yaml:"include"`
		Ignore       []string      `yaml:"ignore"`
		Tags         []*Tag        `yaml:"tags"`
	}
	if err = unmarshal(&aux); err != nil {
		return
	}
	ss.Name = aux.Name
	ss.OutDir = aux.OutDir
	ss.Threads = aux.Threads
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
	return nil
}

// Tag is the struct for managing configuration options for "tags" (groups) of files.
type Tag struct {
	Pattern   *regexp.Regexp
	Priority  int
	Method    string
	Order     string
	Delete    bool
	LastDelay time.Duration
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s).
// https://godoc.org/gopkg.in/yaml.v2
func (t *Tag) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
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
	if aux.Pattern != DefaultTag {
		if t.Pattern, err = regexp.Compile(aux.Pattern); err != nil {
			return
		}
	} else {
		if aux.Order == "" {
			aux.Order = OrderFIFO
		}
		if aux.Method == "" {
			aux.Method = MethodHTTP
		}
	}
	switch aux.Method {
	case MethodDisk:
		break
	case MethodHTTP:
		break
	case "":
		break
	default:
		panic(fmt.Sprintf("Invalid \"tag\" method: %s", aux.Method))
	}
	t.Priority = aux.Priority
	t.Method = aux.Method
	t.Order = aux.Order
	t.Delete = aux.Delete
	t.LastDelay = aux.LastDelay
	return
}

// OutTarget houses the configuration for the target host for a given source.
type OutTarget struct {
	Name        string `yaml:"name"`
	Key         string `yaml:"key"`
	Host        string `yaml:"http-host"`
	TLSCertPath string `yaml:"http-tls-cert"`
}

// InConf is the struct for housing all incoming configuration.
type InConf struct {
	Sources []string  `yaml:"sources"`
	Keys    []string  `yaml:"keys"`
	Dirs    *InDirs   `yaml:"dirs"`
	Server  *InServer `yaml:"server"`
}

// InDirs is the struct for managing the incoming directory configuration items.
type InDirs struct {
	Logs    string `yaml:"logs"`
	LogsIn  string `yaml:"logs-in"`
	LogsMsg string `yaml:"logs-flow"`
	Stage   string `yaml:"stage"`
	Final   string `yaml:"final"`
}

// InServer is the struct for managing the incoming HTTP host.
type InServer struct {
	Port        int    `yaml:"http-port"`
	TLSCertPath string `yaml:"http-tls-cert"`
	TLSKeyPath  string `yaml:"http-tls-key"`
	Compression int    `yaml:"compress"`
}

// NewConf returns a parsed Conf reference based on the provided conf file path.
func NewConf(path string) (*Conf, error) {
	var err error
	conf := &Conf{}
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
		panic(err.Error())
	}
	if conf.Out != nil {
		if len(conf.Out.Sources) > 0 {
			var src *OutSource
			var tgt *OutSource
			for i := 0; i < len(conf.Out.Sources); i++ {
				if src != nil {
					tgt = conf.Out.Sources[i]
					copyStruct(tgt, src)
					if src.Target != nil && tgt.Target != nil {
						copyStruct(tgt.Target, src.Target)
					}
				}
				src = conf.Out.Sources[i]
				if len(src.Tags) > 1 {
					tdef := src.Tags[0]
					for j := 1; j < len(src.Tags); j++ {
						copyStruct(src.Tags[j], tdef)
					}
				}
			}
		}
	}
	conf.path = path
	return conf, nil
}

func copyStruct(tgt interface{}, src interface{}) {
	v := reflect.ValueOf(tgt)
	z := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = reflect.Indirect(v)
		z = reflect.Indirect(z)
	}
	if v.Kind() != reflect.Struct || v.Type() != z.Type() {
		return
	}
	for i := 0; i < v.NumField(); i++ {
		if isZero(v.Field(i)) {
			v.Field(i).Set(reflect.ValueOf(z.Field(i).Interface()))
		}
	}
}

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	case reflect.Array:
		z := true
		for i := 0; i < v.Len(); i++ {
			z = z && isZero(v.Index(i))
		}
		return z
	case reflect.Struct:
		z := true
		n := 0
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanSet() {
				z = z && isZero(v.Field(i))
				n++
			}
		}
		if n > 0 {
			return z
		}
		return false
	case reflect.Ptr:
		if v.IsNil() {
			return true
		}
		return isZero(reflect.Indirect(v))
	}
	// Compare other types directly:
	z := reflect.Zero(v.Type())
	return v.Interface() == z.Interface()
}
