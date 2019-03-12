package sts

import (
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts/reflectutil"

	"github.com/alecthomas/units"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultTag = "DEFAULT"

	// OrderFIFO indicates first-in, first-out sorting
	OrderFIFO = "fifo"

	// OrderLIFO indicates last-in, first-out sorting
	OrderLIFO = "lifo"

	// OrderAlpha indicates alphabetic ordering
	OrderAlpha = ""
)

// Conf is the outer struct for decoding a YAML config file
type Conf struct {
	AgentKey string      `yaml:"stackimpact"`
	Client   *ClientConf `yaml:"OUT"`
	Server   *ServerConf `yaml:"IN"`
}

// ClientConf is the struct for housing all outgoing configuration.
type ClientConf struct {
	Dirs    *ClientDirs
	Sources []*SourceConf
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
// This is where we implement the feature of propagating options.
func (conf *ClientConf) UnmarshalYAML(
	unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Dirs    *ClientDirs   `yaml:"dirs"`
		Sources []*SourceConf `yaml:"sources"`
	}
	if err = unmarshal(&aux); err != nil {
		return
	}
	conf.Dirs = aux.Dirs
	conf.Sources = aux.Sources
	if len(conf.Sources) == 0 {
		return
	}
	var src *SourceConf
	var tgt *SourceConf
	for i := 0; i < len(conf.Sources); i++ {
		if src != nil {
			tgt = conf.Sources[i]
			origStat := tgt.StatPayload
			reflectutil.CopyStruct(tgt, src)
			if tgt.isStatPayloadSet {
				tgt.StatPayload = origStat
			}
			if src.Target != nil && tgt.Target != nil {
				reflectutil.CopyStruct(tgt.Target, src.Target)
			}
		}
		src = conf.Sources[i]
		if len(src.Tags) > 1 {
			tdef := src.Tags[0]
			for j := 1; j < len(src.Tags); j++ {
				origDelete := src.Tags[j].Delete
				reflectutil.CopyStruct(src.Tags[j], tdef)
				if src.Tags[j].isDeleteSet {
					src.Tags[j].Delete = origDelete
				}
			}
		}
	}
	return
}

// ClientDirs is the struct for managing the outgoing directory configuration
// items.
type ClientDirs struct {
	Out       string `yaml:"out"`
	OutFollow bool   `yaml:"out-follow"`
	Log       string `yaml:"logs"`
	LogOut    string `yaml:"logs-out"`
	LogMsg    string `yaml:"logs-flow"`
	Cache     string `yaml:"cache"`
}

// SourceConf is the struct for managing the configuration of an outgoing
// client source.
type SourceConf struct {
	Name          string
	OutDir        string
	LogDir        string
	Threads       int
	CacheAge      time.Duration
	MinAge        time.Duration
	MaxAge        time.Duration
	ScanDelay     time.Duration
	Timeout       time.Duration
	BinSize       units.Base2Bytes
	Compression   int
	StatPayload   bool
	StatInterval  time.Duration
	PollDelay     time.Duration
	PollInterval  time.Duration
	PollAttempts  int
	Target        *TargetConf
	GroupBy       *regexp.Regexp
	IncludeHidden bool
	Include       []*regexp.Regexp
	Ignore        []*regexp.Regexp
	Tags          []*TagConf

	// We have to do this mumbo jumbo if we ever want a false value to
	// override a true value because a false boolean value is the "empty"
	// value and it's impossible to know if it was set in the config file or
	// if it was just the default value because it wasn't specified.
	isStatPayloadSet bool
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s).
// https://godoc.org/gopkg.in/yaml.v2
func (ss *SourceConf) UnmarshalYAML(
	unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Name          string        `yaml:"name"`
		OutDir        string        `yaml:"out-dir"`
		LogDir        string        `yaml:"logout-dir"`
		Threads       int           `yaml:"threads"`
		CacheAge      time.Duration `yaml:"cache-age"`
		MinAge        time.Duration `yaml:"min-age"`
		MaxAge        time.Duration `yaml:"max-age"`
		ScanDelay     time.Duration `yaml:"scan-delay"`
		Timeout       time.Duration `yaml:"timeout"`
		BinSize       string        `yaml:"bin-size"`
		Compression   int           `yaml:"compress"`
		StatPayload   string        `yaml:"stat-payload"`
		StatInterval  time.Duration `yaml:"stat-interval"`
		PollDelay     time.Duration `yaml:"poll-delay"`
		PollInterval  time.Duration `yaml:"poll-interval"`
		PollAttempts  int           `yaml:"poll-attempts"`
		Target        *TargetConf   `yaml:"target"`
		GroupBy       string        `yaml:"group-by"`
		IncludeHidden string        `yaml:"include-hidden"`
		Include       []string      `yaml:"include"`
		Ignore        []string      `yaml:"ignore"`
		Tags          []*TagConf    `yaml:"tags"`
	}
	if err = unmarshal(&aux); err != nil {
		return
	}
	ss.Name = aux.Name
	ss.OutDir = aux.OutDir
	ss.LogDir = aux.LogDir
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
	switch {
	case strings.ToLower(aux.StatPayload) == "true":
		ss.StatPayload = true
		break
	case strings.ToLower(aux.StatPayload) == "false":
		ss.isStatPayloadSet = true
		break
	}
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
	switch {
	case strings.ToLower(aux.IncludeHidden) == "true":
		ss.IncludeHidden = true
		break
	case strings.ToLower(aux.IncludeHidden) == "false":
		ss.IncludeHidden = false
		break
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

// TagConf is the struct for managing configuration options for "tags" (groups)
// of files.
type TagConf struct {
	Pattern   *regexp.Regexp
	Priority  int
	Method    string
	Order     string
	Delete    bool
	LastDelay time.Duration

	// We have to do this mumbo jumbo if we ever want a false value to
	// override a true value because a false boolean value is the "empty"
	// value and it's impossible to know if it was set in the config file or
	// if it was just the default value because it wasn't specified.
	isDeleteSet bool
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
func (t *TagConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var aux struct {
		Pattern   string        `yaml:"pattern"`
		Priority  int           `yaml:"priority"`
		Method    string        `yaml:"method"`
		Order     string        `yaml:"order"`
		Delete    string        `yaml:"delete"`
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
			aux.Order = OrderFIFO
		}
	}
	t.Priority = aux.Priority
	t.Method = aux.Method
	t.Order = aux.Order
	switch {
	case strings.ToLower(aux.Delete) == "true":
		t.Delete = true
		break
	case strings.ToLower(aux.Delete) == "false":
		t.Delete = false
		t.isDeleteSet = true
		break
	}
	t.LastDelay = aux.LastDelay
	return
}

// TargetConf houses the configuration for the target host for a given source
type TargetConf struct {
	Name        string `yaml:"name"`
	Key         string `yaml:"key"`
	Host        string `yaml:"http-host"`
	TLSCertPath string `yaml:"http-tls-cert"`
}

// ServerConf is the struct for housing all incoming configuration
type ServerConf struct {
	Sources []string    `yaml:"sources"`
	Keys    []string    `yaml:"keys"`
	Dirs    *ServerDirs `yaml:"dirs"`
	Server  *HTTPServer `yaml:"server"`
}

// ServerDirs is the struct for managing the incoming directory configuration
// items.
type ServerDirs struct {
	Log    string `yaml:"logs"`
	LogIn  string `yaml:"logs-in"`
	LogMsg string `yaml:"logs-flow"`
	Stage  string `yaml:"stage"`
	Final  string `yaml:"final"`
	Serve  string `yaml:"serve"`
}

// HTTPServer is the struct for managing the incoming HTTP host
type HTTPServer struct {
	Host        string `yaml:"http-host"`
	Port        int    `yaml:"http-port"`
	TLSCertPath string `yaml:"http-tls-cert"`
	TLSKeyPath  string `yaml:"http-tls-key"`
	Compression int    `yaml:"compress"`
}

// NewConf returns a parsed Conf reference based on the provided conf file path
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
		return nil, err
	}
	return conf, nil
}

// InitPaths will initialize all conf paths and use the provided root for
// resolving relative paths
func InitPaths(conf *Conf,
	join func(...string) string,
	mkpath func(*string, bool) error) (err error) {
	var dirs []*string
	var files []*string
	if conf.Client != nil {
		SetDefault(map[*string]string{
			&conf.Client.Dirs.Cache:  DefCache,
			&conf.Client.Dirs.Log:    DefLog,
			&conf.Client.Dirs.LogMsg: DefLogMsg,
			&conf.Client.Dirs.LogOut: DefLogOut,
			&conf.Client.Dirs.Out:    DefOut,
		})
		conf.Client.Dirs.LogMsg = join(
			conf.Client.Dirs.Log,
			conf.Client.Dirs.LogMsg)
		conf.Client.Dirs.LogOut = join(
			conf.Client.Dirs.Log,
			conf.Client.Dirs.LogOut)
		dirs = append(dirs,
			&conf.Client.Dirs.Cache,
			&conf.Client.Dirs.Log,
			&conf.Client.Dirs.LogMsg,
			&conf.Client.Dirs.LogOut,
			&conf.Client.Dirs.Out)
		for _, src := range conf.Client.Sources {
			if src.Target.Name == "" {
				src.Target.Name = src.Target.Host
			}
			SetDefault(map[*string]string{
				&src.OutDir: join(
					conf.Client.Dirs.Out, src.Target.Name),
				&src.LogDir: join(
					conf.Client.Dirs.LogOut, src.Target.Name),
			})
			dirs = append(dirs,
				&src.OutDir,
				&src.LogDir)
			files = append(files,
				&src.Target.TLSCertPath)
		}
	}
	if conf.Server != nil {
		SetDefault(map[*string]string{
			&conf.Server.Dirs.Stage:  DefStage,
			&conf.Server.Dirs.Final:  DefFinal,
			&conf.Server.Dirs.Log:    DefLog,
			&conf.Server.Dirs.LogMsg: DefLogMsg,
			&conf.Server.Dirs.LogIn:  DefLogIn,
		})
		conf.Server.Dirs.LogMsg = join(
			conf.Server.Dirs.Log,
			conf.Server.Dirs.LogMsg)
		conf.Server.Dirs.LogIn = join(
			conf.Server.Dirs.Log,
			conf.Server.Dirs.LogIn)
		dirs = append(dirs,
			&conf.Server.Dirs.Log,
			&conf.Server.Dirs.LogMsg,
			&conf.Server.Dirs.LogIn,
			&conf.Server.Dirs.Stage,
			&conf.Server.Dirs.Serve,
			&conf.Server.Dirs.Final)
		files = append(files,
			&conf.Server.Server.TLSKeyPath,
			&conf.Server.Server.TLSCertPath)
	}
	for i, p := range append(dirs, files...) {
		if *p == "" {
			continue
		}
		err = mkpath(p, i < len(dirs))
		if err != nil {
			return
		}
	}
	return
}

// SetDefault will take a map of string pointers and set the values to the
// corresponding string in the map if it is empty
func SetDefault(def map[*string]string) {
	for sPtr, defaultStr := range def {
		if *sPtr == "" {
			*sPtr = defaultStr
		}
	}
}
