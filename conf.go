package main

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"time"

	"github.com/alecthomas/units"
	"gopkg.in/yaml.v2"
)

const (
	// HeaderSourceName is the custom HTTP header for communicating the name of the sending host.
	HeaderSourceName = "X-STS-SourceName"

	// HeaderBinData is the custom HTTP header that houses the JSON-encoded metadata for a bin.
	HeaderBinData = "X-STS-BinData"

	// DefaultTag is the name of the "default" tag.
	DefaultTag = "DEFAULT"

	// OrderNone indicates order is not important for outgoing files.
	OrderNone = "none"

	// OrderFIFO indicates a first in, first out ordering for outgoing files.
	OrderFIFO = "fifo"

	// MethodHTTP indicates an HTTP outgoing method.
	MethodHTTP = "http"

	// MethodDisk indicates a disk transfer method.
	MethodDisk = "disk"

	// MethodNone indicates files are not to be sent.
	MethodNone = "none" // TODO: not currently supported.
)

// Conf is the outermost struct for holding STS configuration.
type Conf struct {
	Send    *SendConf    `yaml:"SEND"`
	Receive *ReceiveConf `yaml:"RECEIVE"`

	path string
}

// SendConf is the struct for housing all outgoing configuration.
type SendConf struct {
	Dirs    *SendDirs     `yaml:"dirs"`
	Sources []*SendSource `yaml:"sources"`
}

// SendDirs is the struct for managing the outgoing directory configuration items.
type SendDirs struct {
	Out   string `yaml:"out"`
	Logs  string `yaml:"logs"`
	Disk  string `yaml:"disk"`
	Cache string `yaml:"cache"`
}

// SendSource is the struct for managing the configuration of an outgoing source.
type SendSource struct {
	Name         string
	Threads      int
	MinAge       time.Duration
	MaxRetries   int
	Timeout      time.Duration
	BinSize      units.Base2Bytes
	Compress     bool
	PollDelay    time.Duration
	PollInterval time.Duration
	Target       *SendTarget
	Tags         []*Tag
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s).
// https://godoc.org/gopkg.in/yaml.v2
func (ss *SendSource) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Name         string        `yaml:"name"`
		Threads      int           `yaml:"threads"`
		MinAge       time.Duration `yaml:"min-age"`
		MaxRetries   int           `yaml:"max-retries"`
		Timeout      time.Duration `yaml:"timeout"`
		BinSize      string        `yaml:"bin-size"`
		Compress     bool          `yaml:"compress"`
		PollDelay    time.Duration `yaml:"poll-delay"`
		PollInterval time.Duration `yaml:"poll-interval"`
		Target       *SendTarget   `yaml:"target"`
		Tags         []*Tag        `yaml:"tags"`
	}
	if err = unmarshal(&aux); err != nil {
		return
	}
	ss.Name = aux.Name
	ss.Threads = aux.Threads
	ss.MinAge = aux.MinAge
	ss.MaxRetries = aux.MaxRetries
	ss.Timeout = aux.Timeout
	if ss.BinSize, err = units.ParseBase2Bytes(aux.BinSize); err != nil {
		return
	}
	ss.Compress = aux.Compress
	ss.PollDelay = aux.PollDelay
	ss.PollInterval = aux.PollInterval
	ss.Target = aux.Target
	ss.Tags = aux.Tags
	return nil
}

// Tag is the struct for managing configuration options for "tags" (groups) of files.
type Tag struct {
	Pattern  string `yaml:"pattern"`
	Priority int    `yaml:"priority"`
	Method   string `yaml:"method"`
	Order    string `yaml:"order"`
	Delete   bool   `yaml:"delete"`
}

// SendTarget houses the configuration for the target host for a given source.
type SendTarget struct {
	Name    string `yaml:"name"`
	Host    string `yaml:"http-host"`
	TLS     bool   `yaml:"http-tls"`
	TLSCert string `yaml:"http-tls-cert"`
	TLSKey  string `yaml:"http-tls-key"`
}

// ReceiveConf is the struct for housing all incoming configuration.
type ReceiveConf struct {
	Dirs   *ReceiveDirs   `yaml:"dirs"`
	Server *ReceiveServer `yaml:"server"`
}

// ReceiveDirs is the struct for managing the incoming directory configuration items.
type ReceiveDirs struct {
	Logs  string `yaml:"logs"`
	Cache string `yaml:"cache"`
	Stage string `yaml:"stage"`
	Final string `yaml:"final"`
}

// ReceiveServer is the struct for managing the incoming HTTP host.
type ReceiveServer struct {
	Port    int    `yaml:"http-port"`
	TLS     bool   `yaml:"http-tls"`
	TLSCert string `yaml:"http-tls-cert"`
	TLSKey  string `yaml:"http-tls-key"`
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
	if conf.Send != nil {
		if len(conf.Send.Sources) > 0 {
			var src *SendSource
			var tgt *SendSource
			for i := 0; i < len(conf.Send.Sources); i++ {
				if src != nil {
					tgt = conf.Send.Sources[i]
					copyStruct(tgt, src)
				}
				src = conf.Send.Sources[i]
				if len(src.Tags) > 1 {
					tdef := src.Tags[0]
					for i := 1; i < len(src.Tags); i++ {
						copyStruct(src.Tags[i], tdef)
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
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanSet() {
				z = z && isZero(v.Field(i))
			}
		}
		return z
	case reflect.Ptr:
		return isZero(reflect.Indirect(v))
	}
	// Compare other types directly:
	z := reflect.Zero(v.Type())
	return v.Interface() == z.Interface()
}
