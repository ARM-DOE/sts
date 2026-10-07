package sts

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/arm-doe/sts/marshal"
	"github.com/arm-doe/sts/reflectutil"

	"github.com/alecthomas/units"
	"go.bryk.io/pkg/net/middleware/hsts"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultTag = "DEFAULT"

	KCPTuningBalanced = "balanced"
	KCPTuningManual   = "manual"

	// OrderFIFO indicates first-in, first-out sorting
	OrderFIFO = "fifo"

	// OrderLIFO indicates last-in, first-out sorting
	OrderLIFO = "lifo"

	// OrderNone indicates ordering is not important
	OrderNone = "none"

	// OrderAlpha indicates alphabetic ordering
	OrderAlpha = ""
)

const (
	defaultKCPPort         = 1993
	defaultKCPMTU          = 1350
	defaultKCPWindow       = 4096
	defaultKCPDataShards   = 20
	defaultKCPParityShards = 2
	defaultKCPInterval     = 20 * time.Millisecond
	defaultKCPFastResend   = 2
	defaultKCPSocketBuffer = 64 * 1024 * 1024
	defaultKCPIdleTimeout  = 2 * time.Minute
)

// KCPConf configures HTTP/1.1 carried over a KCP reliable UDP session.
type KCPConf struct {
	Host                     string        `yaml:"host" json:"host"`
	Port                     int           `yaml:"port" json:"port"`
	Tuning                   string        `yaml:"tuning" json:"tuning"`
	MTU                      int           `yaml:"mtu" json:"mtu"`
	SendWindow               int           `yaml:"send-window" json:"send-window"`
	ReceiveWindow            int           `yaml:"receive-window" json:"receive-window"`
	FECEnabled               bool          `yaml:"fec-enabled" json:"fec-enabled"`
	DataShards               int           `yaml:"data-shards" json:"data-shards"`
	ParityShards             int           `yaml:"parity-shards" json:"parity-shards"`
	DuplicatePackets         int           `yaml:"duplicate-packets" json:"duplicate-packets"`
	NoDelay                  bool          `yaml:"no-delay" json:"no-delay"`
	Interval                 time.Duration `yaml:"interval" json:"interval"`
	FastResend               int           `yaml:"fast-resend" json:"fast-resend"`
	DisableCongestionControl bool          `yaml:"disable-congestion-control" json:"disable-congestion-control"`
	ACKNoDelay               bool          `yaml:"ack-no-delay" json:"ack-no-delay"`
	SocketReadBuffer         uint64        `yaml:"socket-read-buffer" json:"socket-read-buffer"`
	SocketWriteBuffer        uint64        `yaml:"socket-write-buffer" json:"socket-write-buffer"`
	IdleTimeout              time.Duration `yaml:"idle-timeout" json:"idle-timeout"`
	isPortSet                bool
}

type auxKCPConf struct {
	Host                     string           `yaml:"host" json:"host"`
	Port                     int              `yaml:"port" json:"port"`
	Tuning                   string           `yaml:"tuning" json:"tuning"`
	MTU                      int              `yaml:"mtu" json:"mtu"`
	SendWindow               int              `yaml:"send-window" json:"send-window"`
	ReceiveWindow            int              `yaml:"receive-window" json:"receive-window"`
	FECEnabled               *bool            `yaml:"fec-enabled" json:"fec-enabled"`
	DataShards               int              `yaml:"data-shards" json:"data-shards"`
	ParityShards             int              `yaml:"parity-shards" json:"parity-shards"`
	DuplicatePackets         int              `yaml:"duplicate-packets" json:"duplicate-packets"`
	NoDelay                  *bool            `yaml:"no-delay" json:"no-delay"`
	Interval                 marshal.Duration `yaml:"interval" json:"interval"`
	FastResend               int              `yaml:"fast-resend" json:"fast-resend"`
	DisableCongestionControl bool             `yaml:"disable-congestion-control" json:"disable-congestion-control"`
	ACKNoDelay               *bool            `yaml:"ack-no-delay" json:"ack-no-delay"`
	SocketReadBuffer         interface{}      `yaml:"socket-read-buffer" json:"socket-read-buffer"`
	SocketWriteBuffer        interface{}      `yaml:"socket-write-buffer" json:"socket-write-buffer"`
	IdleTimeout              marshal.Duration `yaml:"idle-timeout" json:"idle-timeout"`
}

func (c *KCPConf) applyAux(aux *auxKCPConf) (err error) {
	c.Host = aux.Host
	c.Port = aux.Port
	c.isPortSet = aux.Port != 0
	c.Tuning = strings.ToLower(aux.Tuning)
	if c.Tuning == "" {
		c.Tuning = KCPTuningBalanced
	}
	if c.Port == 0 {
		c.Port = defaultKCPPort
	}
	c.MTU = aux.MTU
	if c.MTU == 0 {
		c.MTU = defaultKCPMTU
	}
	c.SendWindow = aux.SendWindow
	if c.SendWindow == 0 {
		c.SendWindow = defaultKCPWindow
	}
	c.ReceiveWindow = aux.ReceiveWindow
	if c.ReceiveWindow == 0 {
		c.ReceiveWindow = defaultKCPWindow
	}
	c.FECEnabled = true
	if aux.FECEnabled != nil {
		c.FECEnabled = *aux.FECEnabled
	}
	c.DataShards = aux.DataShards
	c.ParityShards = aux.ParityShards
	if c.FECEnabled && c.DataShards == 0 && c.ParityShards == 0 {
		c.DataShards = defaultKCPDataShards
		c.ParityShards = defaultKCPParityShards
	}
	if !c.FECEnabled {
		c.DataShards = 0
		c.ParityShards = 0
	}
	c.DuplicatePackets = aux.DuplicatePackets
	c.NoDelay = true
	if aux.NoDelay != nil {
		c.NoDelay = *aux.NoDelay
	}
	c.Interval = aux.Interval.Duration
	if c.Interval == 0 {
		c.Interval = defaultKCPInterval
	}
	c.FastResend = aux.FastResend
	if c.FastResend == 0 {
		c.FastResend = defaultKCPFastResend
	}
	c.DisableCongestionControl = aux.DisableCongestionControl
	c.ACKNoDelay = true
	if aux.ACKNoDelay != nil {
		c.ACKNoDelay = *aux.ACKNoDelay
	}
	if c.SocketReadBuffer, err = parseByteCount(aux.SocketReadBuffer, "socket-read-buffer"); err != nil {
		return
	}
	if c.SocketReadBuffer == 0 {
		c.SocketReadBuffer = defaultKCPSocketBuffer
	}
	if c.SocketWriteBuffer, err = parseByteCount(aux.SocketWriteBuffer, "socket-write-buffer"); err != nil {
		return
	}
	if c.SocketWriteBuffer == 0 {
		c.SocketWriteBuffer = defaultKCPSocketBuffer
	}
	c.IdleTimeout = aux.IdleTimeout.Duration
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultKCPIdleTimeout
	}
	return
}

func (c *KCPConf) UnmarshalYAML(unmarshal func(interface{}) error) error {
	aux := &auxKCPConf{}
	if err := unmarshal(aux); err != nil {
		return err
	}
	return c.applyAux(aux)
}

func (c *KCPConf) UnmarshalJSON(data []byte) error {
	aux := &auxKCPConf{}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	return c.applyAux(aux)
}

// ApplyDefaultPort replaces the KCP fallback when no explicit port was configured.
func (c *KCPConf) ApplyDefaultPort(port int) {
	if !c.isPortSet {
		c.Port = port
	}
}

// Validate checks values constrained by KCP's wire format and session APIs.
func (c *KCPConf) Validate() error {
	if c.Tuning != KCPTuningBalanced && c.Tuning != KCPTuningManual {
		return fmt.Errorf("kcp tuning must be %q or %q", KCPTuningBalanced, KCPTuningManual)
	}
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("kcp port must be between 1 and 65535")
	}
	if c.MTU < 576 || c.MTU > 1500 {
		return fmt.Errorf("kcp mtu must be between 576 and 1500 bytes")
	}
	if c.SendWindow < 1 || c.SendWindow > 65535 || c.ReceiveWindow < 1 || c.ReceiveWindow > 65535 {
		return fmt.Errorf("kcp send and receive windows must be between 1 and 65535 packets")
	}
	if c.DataShards < 0 || c.ParityShards < 0 || c.DataShards+c.ParityShards > 255 {
		return fmt.Errorf("kcp FEC shards must be non-negative and total no more than 255")
	}
	if (c.DataShards == 0) != (c.ParityShards == 0) {
		return fmt.Errorf("kcp data-shards and parity-shards must both be zero or both be positive")
	}
	if c.DuplicatePackets < 0 || c.DuplicatePackets > 10 {
		return fmt.Errorf("kcp duplicate-packets must be between 0 and 10")
	}
	if c.FECEnabled && c.DuplicatePackets > 0 {
		return fmt.Errorf("kcp duplicate-packets cannot be combined with FEC")
	}
	if c.Interval < 10*time.Millisecond || c.Interval > 5*time.Second {
		return fmt.Errorf("kcp interval must be between 10ms and 5s")
	}
	if c.FastResend < 0 {
		return fmt.Errorf("kcp fast-resend cannot be negative")
	}
	if c.SocketReadBuffer == 0 || c.SocketWriteBuffer == 0 {
		return fmt.Errorf("kcp socket buffers must be positive")
	}
	maxInt := uint64(^uint(0) >> 1)
	if c.SocketReadBuffer > maxInt || c.SocketWriteBuffer > maxInt {
		return fmt.Errorf("kcp socket buffers exceed platform integer capacity")
	}
	if c.IdleTimeout <= 0 {
		return fmt.Errorf("kcp idle-timeout must be positive")
	}
	return nil
}

// Conf is the outer struct for decoding a YAML config file
type Conf struct {
	AgentKey string      `yaml:"stackimpact" json:"stackimpact"`
	Client   *ClientConf `yaml:"OUT" json:"out"`
	Server   *ServerConf `yaml:"IN" json:"in"`
}

// ClientConf is the struct for housing all outgoing configuration
type ClientConf struct {
	Dirs    *ClientDirs   `yaml:"dirs" json:"dirs"`
	Sources []*SourceConf `yaml:"sources" json:"sources"`
}

// propagate fills in missing options in sources[1..]
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
			origBackoff := tgt.ErrorBackoff
			reflectutil.CopyStruct(tgt, src)
			if tgt.isStatPayloadSet {
				tgt.StatPayload = origStat
			}
			if tgt.isErrorBackoffSet {
				tgt.ErrorBackoff = origBackoff
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

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s)
// https://godoc.org/gopkg.in/yaml.v2
func (conf *ClientConf) UnmarshalYAML(
	unmarshal func(interface{}) error,
) (err error) {
	aux := (*aliasClientConf)(conf)
	if err = unmarshal(aux); err != nil {
		return
	}
	conf.propagate()
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Unmarshal
func (conf *ClientConf) UnmarshalJSON(data []byte) (err error) {
	aux := (*aliasClientConf)(conf)
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	conf.propagate()
	return
}

// ClientDirs is the struct for managing the outgoing directory configuration items
type ClientDirs struct {
	Out       string `yaml:"out" json:"out"`
	OutFollow bool   `yaml:"out-follow" json:"out-follow"`
	Log       string `yaml:"logs" json:"logs"`
	LogOut    string `yaml:"logs-out" json:"logs-out"`
	LogMsg    string `yaml:"logs-flow" json:"logs-flow"`
	Cache     string `yaml:"cache" json:"cache"`
}

// SourceConf is the struct for managing the configuration of an outgoing client source
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
	Compression   int
	StatInterval  time.Duration
	PollDelay     time.Duration
	PollInterval  time.Duration
	PollAttempts  int
	PollMaxCount  int
	Target        *TargetConf
	Rename        []*MappingConf
	Tags          []*TagConf
	BinSize       units.Base2Bytes
	StatPayload   bool
	GroupBy       *regexp.Regexp
	IncludeHidden bool
	Include       []*regexp.Regexp
	Ignore        []*regexp.Regexp
	ErrorBackoff  float64

	// We have to do this mumbo jumbo if we ever want a false value to
	// override a true value because a false boolean value is the "empty"
	// value and it's impossible to know if it was set in the config file or
	// if it was just the default value because it wasn't specified.
	isStatPayloadSet  bool
	isErrorBackoffSet bool
}

func (c *SourceConf) GenMappingVars() map[string]string {
	return map[string]string{
		"__source": c.Name,
	}
}

type auxSourceConf struct {
	Name          string           `yaml:"name" json:"name"`
	OutDir        string           `yaml:"out-dir" json:"out-dir"`
	LogDir        string           `yaml:"log-dir" json:"log-dir"`
	Threads       int              `yaml:"threads" json:"threads"`
	CacheAge      marshal.Duration `yaml:"cache-age" json:"cache-age"`
	MinAge        marshal.Duration `yaml:"min-age" json:"min-age"`
	MaxAge        marshal.Duration `yaml:"max-age" json:"max-age"`
	ScanDelay     marshal.Duration `yaml:"scan-delay" json:"scan-delay"`
	Timeout       marshal.Duration `yaml:"timeout" json:"timeout"`
	Compression   int              `yaml:"compress" json:"compress"`
	StatInterval  marshal.Duration `yaml:"stat-interval" json:"stat-interval"`
	PollDelay     marshal.Duration `yaml:"poll-delay" json:"poll-delay"`
	PollInterval  marshal.Duration `yaml:"poll-interval" json:"poll-interval"`
	PollAttempts  int              `yaml:"poll-attempts" json:"poll-attempts"`
	PollMaxCount  int              `yaml:"poll-max-count" json:"poll-max-count"`
	Target        *TargetConf      `yaml:"target" json:"target"`
	Rename        []*MappingConf   `yaml:"rename" json:"rename"`
	Tags          []*TagConf       `yaml:"tags" json:"tags"`
	BinSize       string           `yaml:"bin-size" json:"bin-size"`
	StatPayload   string           `yaml:"stat-payload" json:"stat-payload"`
	GroupBy       string           `yaml:"group-by" json:"group-by"`
	IncludeHidden string           `yaml:"include-hidden" json:"include-hidden"`
	Include       []string         `yaml:"include" json:"include"`
	Ignore        []string         `yaml:"ignore" json:"ignore"`
	ErrorBackoff  string           `yaml:"error-backoff" json:"error-backoff"`
}

func (ss *SourceConf) applyAux(aux *auxSourceConf) (err error) {
	ss.Name = aux.Name
	ss.OutDir = aux.OutDir
	ss.LogDir = aux.LogDir
	ss.Threads = aux.Threads
	ss.CacheAge = aux.CacheAge.Duration
	ss.MinAge = aux.MinAge.Duration
	ss.MaxAge = aux.MaxAge.Duration
	ss.ScanDelay = aux.ScanDelay.Duration
	ss.Timeout = aux.Timeout.Duration
	ss.Compression = aux.Compression
	ss.StatInterval = aux.StatInterval.Duration
	ss.PollDelay = aux.PollDelay.Duration
	ss.PollInterval = aux.PollInterval.Duration
	ss.PollAttempts = aux.PollAttempts
	ss.PollMaxCount = aux.PollMaxCount
	ss.Target = aux.Target
	ss.Rename = aux.Rename
	ss.Tags = aux.Tags
	if aux.BinSize != "" {
		if ss.BinSize, err = units.ParseBase2Bytes(aux.BinSize); err != nil {
			return
		}
	}
	switch {
	case strings.ToLower(aux.StatPayload) == "true":
		ss.StatPayload = true
	case strings.ToLower(aux.StatPayload) == "false":
		ss.isStatPayloadSet = true
	}
	if aux.GroupBy != "" {
		if ss.GroupBy, err = regexp.Compile(aux.GroupBy); err != nil {
			return
		}
	}
	switch {
	case strings.ToLower(aux.IncludeHidden) == "true":
		ss.IncludeHidden = true
	case strings.ToLower(aux.IncludeHidden) == "false":
		ss.IncludeHidden = false
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
	ss.Ignore = patterns[len(aux.Include):]
	if aux.ErrorBackoff != "" {
		ss.ErrorBackoff, err = strconv.ParseFloat(aux.ErrorBackoff, 64)
		ss.isErrorBackoffSet = true
	}
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s)
// https://godoc.org/gopkg.in/yaml.v2
func (ss *SourceConf) UnmarshalYAML(
	unmarshal func(interface{}) error,
) (err error) {
	aux := &auxSourceConf{}
	if err = unmarshal(aux); err != nil {
		return
	}
	err = ss.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Unmarshal
func (ss *SourceConf) UnmarshalJSON(data []byte) (err error) {
	aux := &auxSourceConf{}
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	err = ss.applyAux(aux)
	return
}

// MarshalJSON implements Marshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Marshal
func (ss *SourceConf) MarshalJSON() ([]byte, error) {
	aux := &auxSourceConf{}
	aux.Name = ss.Name
	aux.OutDir = ss.OutDir
	aux.LogDir = ss.LogDir
	aux.Threads = ss.Threads
	aux.CacheAge.Duration = ss.CacheAge
	aux.MinAge.Duration = ss.MinAge
	aux.MaxAge.Duration = ss.MaxAge
	aux.ScanDelay.Duration = ss.ScanDelay
	aux.Timeout.Duration = ss.Timeout
	aux.Compression = ss.Compression
	aux.StatInterval.Duration = ss.StatInterval
	aux.PollDelay.Duration = ss.PollDelay
	aux.PollInterval.Duration = ss.PollInterval
	aux.PollAttempts = ss.PollAttempts
	aux.PollMaxCount = ss.PollMaxCount
	aux.Target = ss.Target
	aux.Rename = ss.Rename
	aux.Tags = ss.Tags
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
	aux.Ignore = strings[len(ss.Include):]
	if ss.isErrorBackoffSet {
		aux.ErrorBackoff = fmt.Sprintf("%f", ss.ErrorBackoff)
	}
	return json.Marshal(aux)
}

// MappingConf is the struct for indicating what path pattern should be mapped
// to a different target name based on the associated template string
type MappingConf struct {
	Pattern  *regexp.Regexp
	Template string
}

type auxMappingConf struct {
	Pattern  string `yaml:"from" json:"from"`
	Template string `yaml:"to" json:"to"`
}

func (m *MappingConf) applyAux(aux *auxMappingConf) (err error) {
	if m.Pattern, err = regexp.Compile(aux.Pattern); err != nil {
		return
	}
	m.Template = aux.Template
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom
// member(s): https://godoc.org/gopkg.in/yaml.v2
func (m *MappingConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	aux := &auxMappingConf{}
	if err = unmarshal(aux); err != nil {
		return
	}
	err = m.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Unmarshal
func (m *MappingConf) UnmarshalJSON(data []byte) (err error) {
	aux := &auxMappingConf{}
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	err = m.applyAux(aux)
	return
}

// MarshalJSON implements Marshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Marshal
func (m *MappingConf) MarshalJSON() ([]byte, error) {
	aux := &auxMappingConf{}
	aux.Template = m.Template
	aux.Pattern = m.Pattern.String()
	return json.Marshal(aux)
}

// TagConf is the struct for managing configuration options for "tags" (groups) of files
type TagConf struct {
	Priority    int
	Method      string
	Order       string
	Pattern     *regexp.Regexp
	ChunkSize   units.Base2Bytes
	Delete      bool
	LastDelay   time.Duration
	DeleteDelay time.Duration

	// We have to do this mumbo jumbo if we ever want a false value to
	// override a true value because a false boolean value is the "empty"
	// value and it's impossible to know if it was set in the config file or
	// if it was just the default value because it wasn't specified.
	isDeleteSet bool
}

type auxTagConf struct {
	Priority    int              `yaml:"priority" json:"priority"`
	Method      string           `yaml:"method" json:"method"`
	Order       string           `yaml:"order" json:"order"`
	Pattern     string           `yaml:"pattern" json:"pattern"`
	ChunkSize   string           `yaml:"chunk-size" json:"chunk-size"`
	Delete      string           `yaml:"delete" json:"delete"`
	LastDelay   marshal.Duration `yaml:"last-delay" json:"last-delay"`
	DeleteDelay marshal.Duration `yaml:"delete-delay" json:"delete-delay"`
}

func (t *TagConf) applyAux(aux *auxTagConf) (err error) {
	t.Priority = aux.Priority
	t.Method = aux.Method
	t.Order = aux.Order
	t.LastDelay = aux.LastDelay.Duration
	t.DeleteDelay = aux.DeleteDelay.Duration
	if aux.Pattern != defaultTag && aux.Pattern != "" {
		if t.Pattern, err = regexp.Compile(aux.Pattern); err != nil {
			return
		}
	}
	if aux.ChunkSize != "" {
		if t.ChunkSize, err = units.ParseBase2Bytes(aux.ChunkSize); err != nil {
			return
		}
	}
	switch {
	case strings.ToLower(aux.Delete) == "true":
		t.Delete = true
	case strings.ToLower(aux.Delete) == "false":
		t.Delete = false
		t.isDeleteSet = true
	}
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s)
// https://godoc.org/gopkg.in/yaml.v2
func (t *TagConf) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	aux := &auxTagConf{}
	if err = unmarshal(aux); err != nil {
		return
	}
	err = t.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Unmarshal
func (t *TagConf) UnmarshalJSON(data []byte) (err error) {
	aux := &auxTagConf{}
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	err = t.applyAux(aux)
	return
}

// MarshalJSON implements Marshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Marshal
func (t *TagConf) MarshalJSON() ([]byte, error) {
	aux := &auxTagConf{}
	aux.Priority = t.Priority
	aux.Method = t.Method
	aux.Order = t.Order
	if t.Pattern != nil {
		aux.Pattern = t.Pattern.String()
	}
	aux.ChunkSize = t.ChunkSize.String()
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

// QUICConf configures QUIC transport behavior for HTTP3.
type QUICConf struct {
	MaxStreams                 int64         `yaml:"max-streams" json:"max-streams"`
	MaxIdleTimeout             time.Duration `yaml:"max-idle-timeout" json:"max-idle-timeout"`
	KeepAlive                  time.Duration `yaml:"keep-alive" json:"keep-alive"`
	MaxStreamReceiveWindow     uint64        `yaml:"max-stream-receive-window" json:"max-stream-receive-window"`
	MaxConnectionReceiveWindow uint64        `yaml:"max-connection-receive-window" json:"max-connection-receive-window"`
	EnableDatagrams            bool          `yaml:"enable-datagrams" json:"enable-datagrams"`
	DisablePathMTUDiscovery    bool          `yaml:"disable-path-mtu-discovery" json:"disable-path-mtu-discovery"`
	Disable0RTT                bool          `yaml:"disable-0rtt" json:"disable-0rtt"`
}

type auxQUICConf struct {
	MaxStreams                 int64         `yaml:"max-streams" json:"max-streams"`
	MaxIdleTimeout             time.Duration `yaml:"max-idle-timeout" json:"max-idle-timeout"`
	KeepAlive                  time.Duration `yaml:"keep-alive" json:"keep-alive"`
	MaxStreamReceiveWindow     interface{}   `yaml:"max-stream-receive-window" json:"max-stream-receive-window"`
	MaxConnectionReceiveWindow interface{}   `yaml:"max-connection-receive-window" json:"max-connection-receive-window"`
	EnableDatagrams            bool          `yaml:"enable-datagrams" json:"enable-datagrams"`
	DisablePathMTUDiscovery    bool          `yaml:"disable-path-mtu-discovery" json:"disable-path-mtu-discovery"`
	Disable0RTT                bool          `yaml:"disable-0rtt" json:"disable-0rtt"`
}

// TargetConf houses the configuration for the target host for a given source
type TargetConf struct {
	Name          string   `yaml:"name" json:"name"`
	Key           string   `yaml:"key" json:"key"`
	Host          string   `yaml:"http-host" json:"http-host"`
	PathPrefix    string   `yaml:"http-path-prefix" json:"http-path-prefix"`
	TLSCertPath   string   `yaml:"http-tls-cert" json:"http-tls-cert"`
	TLSCertBase64 string   `yaml:"http-tls-cert-encoded" json:"http-tls-cert-encoded"`
	KCP           *KCPConf `yaml:"kcp" json:"kcp"`
	// HTTP3/QUIC support
	Protocol  string    `yaml:"protocol" json:"protocol"`     // "auto", "http3", "https", "http", "kcp-http"
	HTTP3Port int       `yaml:"http3-port" json:"http3-port"` // Optional separate port for HTTP3
	QUIC      *QUICConf `yaml:"quic" json:"quic"`
}

type auxTargetConf struct {
	Name          string    `yaml:"name" json:"name"`
	Key           string    `yaml:"key" json:"key"`
	Host          string    `yaml:"http-host" json:"http-host"`
	PathPrefix    string    `yaml:"http-path-prefix" json:"http-path-prefix"`
	TLSCertPath   string    `yaml:"http-tls-cert" json:"http-tls-cert"`
	TLSCertBase64 string    `yaml:"http-tls-cert-encoded" json:"http-tls-cert-encoded"`
	KCP           *KCPConf  `yaml:"kcp" json:"kcp"`
	Protocol      string    `yaml:"protocol" json:"protocol"`
	HTTP3Port     int       `yaml:"http3-port" json:"http3-port"`
	QUIC          *QUICConf `yaml:"quic" json:"quic"`
}

func parseByteCount(val interface{}, field string) (uint64, error) {
	switch v := val.(type) {
	case nil:
		return 0, nil
	case int:
		if v < 0 {
			return 0, fmt.Errorf("%s cannot be negative", field)
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("%s cannot be negative", field)
		}
		return uint64(v), nil
	case uint64:
		return v, nil
	case float64:
		if v < 0 {
			return 0, fmt.Errorf("%s cannot be negative", field)
		}
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("%s must be a whole number of bytes", field)
		}
		return uint64(v), nil
	case string:
		if v == "" {
			return 0, nil
		}
		n, err := units.ParseBase2Bytes(v)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %s: %w", field, err)
		}
		if n < 0 {
			return 0, fmt.Errorf("%s cannot be negative", field)
		}
		return uint64(n), nil
	default:
		return 0, fmt.Errorf("invalid type %T for %s", val, field)
	}
}

func (c *QUICConf) applyAux(aux *auxQUICConf) (err error) {
	c.MaxStreams = aux.MaxStreams
	c.MaxIdleTimeout = aux.MaxIdleTimeout
	c.KeepAlive = aux.KeepAlive
	if c.MaxStreamReceiveWindow, err = parseByteCount(
		aux.MaxStreamReceiveWindow,
		"quic.max-stream-receive-window"); err != nil {
		return
	}
	if c.MaxConnectionReceiveWindow, err = parseByteCount(
		aux.MaxConnectionReceiveWindow,
		"quic.max-connection-receive-window"); err != nil {
		return
	}
	c.EnableDatagrams = aux.EnableDatagrams
	c.DisablePathMTUDiscovery = aux.DisablePathMTUDiscovery
	c.Disable0RTT = aux.Disable0RTT
	return
}

func (c *QUICConf) UnmarshalYAML(unmarshal func(interface{}) error) error {
	aux := &auxQUICConf{}
	if err := unmarshal(aux); err != nil {
		return err
	}
	return c.applyAux(aux)
}

func (c *QUICConf) UnmarshalJSON(data []byte) error {
	aux := &auxQUICConf{}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	return c.applyAux(aux)
}

func (t *TargetConf) applyAux(aux *auxTargetConf) (err error) {
	t.Name = aux.Name
	t.Key = aux.Key
	t.Host = aux.Host
	t.PathPrefix = aux.PathPrefix
	t.TLSCertPath = aux.TLSCertPath
	t.TLSCertBase64 = aux.TLSCertBase64
	t.KCP = aux.KCP
	t.Protocol = aux.Protocol
	t.HTTP3Port = aux.HTTP3Port
	t.QUIC = aux.QUIC
	if t.KCP != nil && t.Host != "" {
		var port int
		_, port, err = t.ParseHost()
		t.KCP.ApplyDefaultPort(port)
	}
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s)
// https://godoc.org/gopkg.in/yaml.v2
func (t *TargetConf) UnmarshalYAML(
	unmarshal func(interface{}) error,
) (err error) {
	aux := &auxTargetConf{}
	if err = unmarshal(aux); err != nil {
		return
	}
	err = t.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Unmarshal
func (t *TargetConf) UnmarshalJSON(data []byte) (err error) {
	aux := &auxTargetConf{}
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	err = t.applyAux(aux)
	return
}

// ParseHost returns the hostname and port split out or uses default port
func (t *TargetConf) ParseHost() (host string, port int, err error) {
	host = t.Host
	port = DefaultPort
	hostParts := strings.Split(host, ":")
	if len(hostParts) > 1 {
		host = hostParts[0]
		if port, err = strconv.Atoi(hostParts[1]); err != nil {
			err = fmt.Errorf("cannot parse port: %s", t.Host)
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
	Queue         *Queue      `yaml:"queue" json:"queue"`
	S3            *S3         `yaml:"s3" json:"s3"`
	PermitLogBuf  bool        `yaml:"log-buffering" json:"log-buffering"`
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

// ServerDirs is the struct for managing the incoming directory configuration items
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
	Host                     string   `yaml:"http-host" json:"http-host"`
	Port                     int      `yaml:"http-port" json:"http-port"`
	PathPrefix               string   `yaml:"http-path-prefix" json:"http-path-prefix"`
	TLSCertPath              string   `yaml:"http-tls-cert" json:"http-tls-cert"`
	TLSKeyPath               string   `yaml:"http-tls-key" json:"http-tls-key"`
	Compression              int      `yaml:"compress" json:"compress"`
	ChanceOfSimulatedFailure float64  `yaml:"chance-of-simulated-failure" json:"chance-of-simulated-failure"`
	HSTSEnabled              bool     `yaml:"hsts-enabled" json:"hsts-enabled"`
	EnableKCP                bool     `yaml:"kcp-enabled" json:"kcp-enabled"`
	KCP                      *KCPConf `yaml:"kcp" json:"kcp"`
	// HTTP3/QUIC support
	EnableHTTP3 bool         `yaml:"http3-enabled" json:"http3-enabled"` // Run HTTP3 server
	HTTP3Port   int          `yaml:"http3-port" json:"http3-port"`       // Separate port or 0 for same
	QUIC        *QUICConf    `yaml:"quic" json:"quic"`
	HSTS        hsts.Options `yaml:"hsts-options" json:"hsts-options"`
}

type auxHTTPServer struct {
	Host                     string       `yaml:"http-host" json:"http-host"`
	Port                     int          `yaml:"http-port" json:"http-port"`
	PathPrefix               string       `yaml:"http-path-prefix" json:"http-path-prefix"`
	TLSCertPath              string       `yaml:"http-tls-cert" json:"http-tls-cert"`
	TLSKeyPath               string       `yaml:"http-tls-key" json:"http-tls-key"`
	Compression              int          `yaml:"compress" json:"compress"`
	ChanceOfSimulatedFailure float64      `yaml:"chance-of-simulated-failure" json:"chance-of-simulated-failure"`
	HSTSEnabled              bool         `yaml:"hsts-enabled" json:"hsts-enabled"`
	EnableKCP                bool         `yaml:"kcp-enabled" json:"kcp-enabled"`
	KCP                      *KCPConf     `yaml:"kcp" json:"kcp"`
	EnableHTTP3              bool         `yaml:"http3-enabled" json:"http3-enabled"`
	HTTP3Port                int          `yaml:"http3-port" json:"http3-port"`
	QUIC                     *QUICConf    `yaml:"quic" json:"quic"`
	HSTS                     hsts.Options `yaml:"hsts-options" json:"hsts-options"`
}

func (h *HTTPServer) applyAux(aux *auxHTTPServer) (err error) {
	h.Host = aux.Host
	h.Port = aux.Port
	h.PathPrefix = aux.PathPrefix
	h.TLSCertPath = aux.TLSCertPath
	h.TLSKeyPath = aux.TLSKeyPath
	h.Compression = aux.Compression
	h.ChanceOfSimulatedFailure = aux.ChanceOfSimulatedFailure
	h.HSTSEnabled = aux.HSTSEnabled
	h.EnableKCP = aux.EnableKCP
	h.KCP = aux.KCP
	h.EnableHTTP3 = aux.EnableHTTP3
	h.HTTP3Port = aux.HTTP3Port
	h.QUIC = aux.QUIC
	h.HSTS = aux.HSTS
	if h.KCP != nil {
		h.KCP.ApplyDefaultPort(h.Port)
	}
	return
}

// UnmarshalYAML implements the Unmarshaler interface for handling custom member(s)
// https://godoc.org/gopkg.in/yaml.v2
func (h *HTTPServer) UnmarshalYAML(
	unmarshal func(interface{}) error,
) (err error) {
	aux := &auxHTTPServer{}
	if err = unmarshal(aux); err != nil {
		return
	}
	err = h.applyAux(aux)
	return
}

// UnmarshalJSON implements the Unmarshaler interface for handling custom member(s)
// https://golang.org/pkg/encoding/json/#Unmarshal
func (h *HTTPServer) UnmarshalJSON(data []byte) (err error) {
	aux := &auxHTTPServer{}
	if err = json.Unmarshal(data, aux); err != nil {
		return
	}
	err = h.applyAux(aux)
	return
}

// S3 is the struct for managing the S3 storage configuration
type S3 struct {
	Region string `yaml:"aws-region" json:"aws-region"`
	Bucket string `yaml:"bucket" json:"bucket"`
	Prefix string `yaml:"prefix" json:"prefix"`
}

// Queue is the struct for managing an AWS queue resource
type Queue struct {
	Region string `yaml:"aws-region" json:"aws-region"`
	Name   string `yaml:"name" json:"name"`
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
	fh, err := os.ReadFile(path)
	if err != nil {
		log.Fatalln("Configuration file not found: ", path)
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yml":
		fallthrough
	case ".yaml":
		err = yaml.Unmarshal(fh, conf)
	case ".json":
		err = json.Unmarshal(fh, conf)
	default:
		log.Fatalln("Unsupported configuration file format:", filepath.Ext(path))
	}
	if err != nil {
		return nil, err
	}
	return conf, nil
}

// InitPaths will initialize all conf paths and use the provided root for resolving
// relative paths
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
		if *p == "" || strings.HasPrefix(*p, "s3://") {
			continue
		}
		err = mkpath(p, i < len(dirs))
		if err != nil {
			return
		}
	}
	return
}

// SetDefault will take a map of string pointers and set the values to the corresponding
// string in the map if it is empty
func SetDefault(def map[*string]string) {
	for sPtr, defaultStr := range def {
		if *sPtr == "" {
			*sPtr = defaultStr
		}
	}
}
