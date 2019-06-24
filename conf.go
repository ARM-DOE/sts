package sts

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strconv"
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

// Duration wraps time.Duration for JSON marshaling
type Duration struct {
	time.Duration
}

// MarshalJSON implements Marshaler interface
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON implements Unmarshaler interface
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

// Conf is the outer struct for decoding a YAML config file
type Conf struct {
	AgentKey string      `yaml:"stackimpact" json:"stackimpact"`
	Client   *ClientConf `yaml:"OUT" json:"out"`
	Server   *ServerConf `yaml:"IN" json:"in"`
}

// ClientConf is the struct for housing all outgoing configuration.
type ClientConf struct {
	Dirs    *ClientDirs   `yaml:"dirs" json:"dirs"`
	Sources []*SourceConf `yaml:"sources" json:"sources"`
}

// propagate fills in missing options in sources[1..].
func (conf *ClientConf) propagate() {
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
}

type aliasClientConf ClientConf

type auxClientConf struct {
	*aliasClientConf
}

func (conf *ClientConf) getAux() *auxClientConf {
	return &auxClientConf{
		aliasClientConf: (*aliasClientConf)(conf),
	}
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
func (conf *ClientConf) UnmarshalYAML(
	unmarshal func(interface{}) error,
) (err error) {
	if err = unmarshal(conf.getAux()); err != nil {
		return
	}
	conf.propagate()
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom
// member(s): https://golang.org/pkg/encoding/json/#Unmarshal
func (conf *ClientConf) UnmarshalJSON(data []byte) (err error) {
	if err = json.Unmarshal(data, conf.getAux()); err != nil {
		return
	}
	conf.propagate()
	return
}

// ClientDirs is the struct for managing the outgoing directory configuration
// items.
type ClientDirs struct {
	Out       string `yaml:"out" json:"out"`
	OutFollow bool   `yaml:"out-follow" json:"out-follow"`
	Log       string `yaml:"logs" json:"logs"`
	LogOut    string `yaml:"logs-out" json:"logs-out"`
	LogMsg    string `yaml:"logs-flow" json:"logs-flow"`
	Cache     string `yaml:"cache" json:"cache"`
}

// SourceConf is the struct for managing the configuration of an outgoing
// client source.
type SourceConf struct {
	Name          string           `yaml:"name" json:"name"`
	OutDir        string           `yaml:"out-dir" json:"out-dir"`
	LogDir        string           `yaml:"log-dir" json:"log-dir"`
	Threads       int              `yaml:"threads" json:"threads"`
	CacheAge      time.Duration    `yaml:"cache-age" json:"-"`
	MinAge        time.Duration    `yaml:"min-age" json:"-"`
	MaxAge        time.Duration    `yaml:"max-age" json:"-"`
	ScanDelay     time.Duration    `yaml:"scan-delay" json:"-"`
	Timeout       time.Duration    `yaml:"timeout" json:"-"`
	Compression   int              `yaml:"compress" json:"compress"`
	StatInterval  time.Duration    `yaml:"stat-interval" json:"-"`
	PollDelay     time.Duration    `yaml:"poll-delay" json:"-"`
	PollInterval  time.Duration    `yaml:"poll-interval" json:"-"`
	PollAttempts  int              `yaml:"poll-attempts" json:"poll-attempts"`
	Target        *TargetConf      `yaml:"target" json:"target"`
	Rename        []*MappingConf   `yaml:"rename" json:"rename"`
	Tags          []*TagConf       `yaml:"tags" json:"tags"`
	BinSize       units.Base2Bytes `json:"-"`
	StatPayload   bool             `json:"-"`
	GroupBy       *regexp.Regexp   `json:"-"`
	IncludeHidden bool             `json:"-"`
	Include       []*regexp.Regexp `json:"-"`
	Ignore        []*regexp.Regexp `json:"-"`

	// We have to do this mumbo jumbo if we ever want a false value to
	// override a true value because a false boolean value is the "empty"
	// value and it's impossible to know if it was set in the config file or
	// if it was just the default value because it wasn't specified.
	isStatPayloadSet bool
}

type aliasSourceConf SourceConf
type auxSourceConf struct {
	BinSize       string   `yaml:"bin-size" json:"bin-size"`
	StatPayload   string   `yaml:"stat-payload" json:"stat-payload"`
	GroupBy       string   `yaml:"group-by" json:"group-by"`
	IncludeHidden string   `yaml:"include-hidden" json:"include-hidden"`
	Include       []string `yaml:"include" json:"include"`
	Ignore        []string `yaml:"ignore" json:"ignore"`
	CacheAge      Duration `json:"cache-age"`
	MinAge        Duration `json:"min-age"`
	MaxAge        Duration `json:"max-age"`
	ScanDelay     Duration `json:"scan-delay"`
	Timeout       Duration `json:"timeout"`
	StatInterval  Duration `json:"stat-interval"`
	PollDelay     Duration `json:"poll-delay"`
	PollInterval  Duration `json:"poll-interval"`
	*aliasSourceConf
}

func (ss *SourceConf) getAux() *auxSourceConf {
	return &auxSourceConf{
		aliasSourceConf: (*aliasSourceConf)(ss),
	}
}

func (ss *SourceConf) applyAux(aux *auxSourceConf) (err error) {
	if aux.BinSize != "" {
		if ss.BinSize, err = units.ParseBase2Bytes(aux.BinSize); err != nil {
			return
		}
	}
	switch {
	case strings.ToLower(aux.StatPayload) == "true":
		ss.StatPayload = true
		break
	case strings.ToLower(aux.StatPayload) == "false":
		ss.isStatPayloadSet = true
		break
	}
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
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s).
// https://godoc.org/gopkg.in/yaml.v2
func (ss *SourceConf) UnmarshalYAML(
	unmarshal func(interface{}) error,
) (err error) {
	aux := ss.getAux()
	if err = unmarshal(aux); err != nil {
		return
	}
	err = ss.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom
// member(s): https://golang.org/pkg/encoding/json/#Unmarshal
func (ss *SourceConf) UnmarshalJSON(data []byte) (err error) {
	aux := ss.getAux()
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	ss.CacheAge = aux.CacheAge.Duration
	ss.MinAge = aux.MinAge.Duration
	ss.MaxAge = aux.MaxAge.Duration
	ss.ScanDelay = aux.ScanDelay.Duration
	ss.Timeout = aux.Timeout.Duration
	ss.StatInterval = aux.StatInterval.Duration
	ss.PollDelay = aux.PollDelay.Duration
	ss.PollInterval = aux.PollInterval.Duration
	err = ss.applyAux(aux)
	return
}

// MarshalJSON implements Marshaler interface for handling custom member(s):
// https://golang.org/pkg/encoding/json/#Marshal
func (ss *SourceConf) MarshalJSON() ([]byte, error) {
	aux := ss.getAux()
	aux.CacheAge.Duration = ss.CacheAge
	aux.MinAge.Duration = ss.MinAge
	aux.MaxAge.Duration = ss.MaxAge
	aux.ScanDelay.Duration = ss.ScanDelay
	aux.Timeout.Duration = ss.Timeout
	aux.StatInterval.Duration = ss.StatInterval
	aux.PollDelay.Duration = ss.PollDelay
	aux.PollInterval.Duration = ss.PollInterval
	aux.BinSize = ss.BinSize.String()
	if ss.GroupBy != nil {
		aux.GroupBy = ss.GroupBy.String()
	}
	switch {
	case ss.StatPayload:
		aux.StatPayload = "true"
	case ss.isStatPayloadSet:
		aux.StatPayload = "false"
	}
	aux.IncludeHidden = "false"
	if ss.IncludeHidden {
		aux.IncludeHidden = "true"
	}
	var strings []string
	for _, p := range append(ss.Include, ss.Ignore...) {
		strings = append(strings, p.String())
	}
	aux.Include = strings[0:len(ss.Include)]
	aux.Ignore = strings[len(ss.Include):len(strings)]
	return json.Marshal(aux)
}

// MappingConf is the struct for indicating what path pattern should be mapped
// to a different target name based on the associated template string.
type MappingConf struct {
	Template string         `yaml:"to" json:"to"`
	Pattern  *regexp.Regexp `json:"-"`
}

type aliasMappingConf MappingConf
type auxMappingConf struct {
	Pattern string `yaml:"from" json:"from"`
	*aliasMappingConf
}

func (m *MappingConf) getAux() *auxMappingConf {
	return &auxMappingConf{
		aliasMappingConf: (*aliasMappingConf)(m),
	}
}

func (m *MappingConf) applyAux(aux *auxMappingConf) (err error) {
	m.Pattern, err = regexp.Compile(aux.Pattern)
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
func (m *MappingConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	aux := m.getAux()
	if err = unmarshal(aux); err != nil {
		return
	}
	err = m.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom
// member(s): https://golang.org/pkg/encoding/json/#Unmarshal
func (m *MappingConf) UnmarshalJSON(data []byte) (err error) {
	aux := m.getAux()
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	err = m.applyAux(aux)
	return
}

// MarshalJSON implements Marshaler interface for handling custom member(s):
// https://golang.org/pkg/encoding/json/#Marshal
func (m *MappingConf) MarshalJSON() ([]byte, error) {
	aux := m.getAux()
	aux.Pattern = m.Pattern.String()
	return json.Marshal(aux)
}

// TagConf is the struct for managing configuration options for "tags" (groups)
// of files.
type TagConf struct {
	Priority    int            `yaml:"priority" json:"priority"`
	Method      string         `yaml:"method" json:"method"`
	Order       string         `yaml:"order" json:"order"`
	LastDelay   time.Duration  `yaml:"last-delay" json:"-"`
	DeleteDelay time.Duration  `yaml:"delete-delay" json:"-"`
	Pattern     *regexp.Regexp `json:"-"`
	Delete      bool           `json:"-"`

	// We have to do this mumbo jumbo if we ever want a false value to
	// override a true value because a false boolean value is the "empty"
	// value and it's impossible to know if it was set in the config file or
	// if it was just the default value because it wasn't specified.
	isDeleteSet bool
}

type aliasTagConf TagConf
type auxTagConf struct {
	Pattern     string   `yaml:"pattern" json:"pattern"`
	Delete      string   `yaml:"delete" json:"delete"`
	LastDelay   Duration `json:"last-delay"`
	DeleteDelay Duration `json:"delete-delay"`
	*aliasTagConf
}

func (t *TagConf) getAux() *auxTagConf {
	return &auxTagConf{
		aliasTagConf: (*aliasTagConf)(t),
	}
}

func (t *TagConf) applyAux(aux *auxTagConf) (err error) {
	if aux.Pattern != defaultTag && aux.Pattern != "" {
		if t.Pattern, err = regexp.Compile(aux.Pattern); err != nil {
			return
		}
	} else {
		if t.Order == "" {
			t.Order = OrderFIFO
		}
	}
	switch {
	case strings.ToLower(aux.Delete) == "true":
		t.Delete = true
		break
	case strings.ToLower(aux.Delete) == "false":
		t.Delete = false
		t.isDeleteSet = true
		break
	}
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
func (t *TagConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	aux := t.getAux()
	if err = unmarshal(aux); err != nil {
		return
	}
	err = t.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom
// member(s): https://golang.org/pkg/encoding/json/#Unmarshal
func (t *TagConf) UnmarshalJSON(data []byte) (err error) {
	aux := t.getAux()
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	t.LastDelay = aux.LastDelay.Duration
	t.DeleteDelay = aux.DeleteDelay.Duration
	err = t.applyAux(aux)
	return
}

// MarshalJSON implements Marshaler interface for handling custom member(s):
// https://golang.org/pkg/encoding/json/#Marshal
func (t *TagConf) MarshalJSON() ([]byte, error) {
	aux := t.getAux()
	if t.Pattern != nil {
		aux.Pattern = t.Pattern.String()
	}
	aux.LastDelay.Duration = t.LastDelay
	aux.DeleteDelay.Duration = t.DeleteDelay
	switch {
	case t.Delete:
		aux.Delete = "true"
	case t.isDeleteSet:
		aux.Delete = "false"
	}
	return json.Marshal(aux)
}

// TargetConf houses the configuration for the target host for a given source
type TargetConf struct {
	Name          string `yaml:"name" json:"name"`
	Key           string `yaml:"key" json:"key"`
	Host          string `yaml:"http-host" json:"http-host"`
	PathPrefix    string `yaml:"http-path-prefix" json:"http-path-prefix"`
	TLSCertPath   string `yaml:"http-tls-cert" json:"http-tls-cert"`
	TLSCertBase64 string `yaml:"http-tls-cert-encoded" json:"http-tls-cert-encoded"`
}

// ParseHost returns the hostname and port split out or uses default port
func (t *TargetConf) ParseHost() (host string, port int, err error) {
	host = t.Host
	port = DefaultPort
	hostParts := strings.Split(host, ":")
	if len(hostParts) > 1 {
		host = hostParts[0]
		if port, err = strconv.Atoi(hostParts[1]); err != nil {
			err = fmt.Errorf("Cannot parse port: %s", t.Host)
		}
	}
	return
}

// ServerConf is the struct for housing all incoming configuration
type ServerConf struct {
	SourceControl *DBConf     `yaml:"control" json:"control"`
	Sources       []string    `yaml:"sources" json:"sources"`
	Keys          []string    `yaml:"keys" json:"keys"`
	Dirs          *ServerDirs `yaml:"dirs" json:"dirs"`
	Server        *HTTPServer `yaml:"server" json:"server"`
}

// DBConf is the struct for defining a database connection
type DBConf struct {
	Host          string `yaml:"host" json:"host"`
	Port          uint   `yaml:"port" json:"port"`
	User          string `yaml:"user" json:"user"`
	Pass          string `yaml:"pass" json:"pass"`
	Name          string `yaml:"name" json:"name"`
	ClientsTable  string `yaml:"table-clients" json:"table-clients"`
	DatasetsTable string `yaml:"table-datasets" json:"table-datasets"`
}

// ServerDirs is the struct for managing the incoming directory configuration
// items.
type ServerDirs struct {
	Log    string `yaml:"logs" json:"logs"`
	LogIn  string `yaml:"logs-in" json:"logs-in"`
	LogMsg string `yaml:"logs-flow" json:"logs-flow"`
	Stage  string `yaml:"stage" json:"stage"`
	Final  string `yaml:"final" json:"final"`
	Serve  string `yaml:"serve" json:"serve"`
}

// HTTPServer is the struct for managing the incoming HTTP host
type HTTPServer struct {
	Host        string `yaml:"http-host" json:"http-host"`
	Port        int    `yaml:"http-port" json:"http-port"`
	PathPrefix  string `yaml:"http-path-prefix" json:"http-path-prefix"`
	TLSCertPath string `yaml:"http-tls-cert" json:"http-tls-cert"`
	TLSKeyPath  string `yaml:"http-tls-key" json:"http-tls-key"`
	Compression int    `yaml:"compress" json:"compress"`
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
	switch filepath.Ext(path) {
	case ".yaml":
		err = yaml.Unmarshal(fh, conf)
	case ".json":
		err = json.Unmarshal(fh, conf)
	}
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
