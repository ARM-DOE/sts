package main

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/cache"
	"github.com/arm-doe/sts/client"
	"github.com/arm-doe/sts/fileutil"
	"github.com/arm-doe/sts/http"
	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/payload"
	"github.com/arm-doe/sts/queue"
	"github.com/arm-doe/sts/stage"
	"github.com/arm-doe/sts/store"
)

func getTLSConf(conf *sts.TargetConf) (*tls.Config, error) {
	if conf.TLSCertPath != "" {
		return http.LoadTLSConf("", "", conf.TLSCertPath)
	}
	if conf.TLSCertBase64 != "" {
		cert, err := base64.StdEncoding.DecodeString(conf.TLSCertBase64)
		if err != nil {
			return nil, err
		}
		return http.TLSConf([]byte{}, []byte{}, cert)
	}
	return nil, nil
}

type clientApp struct {
	dirCache     string
	dirOutFollow bool
	conf         *sts.SourceConf
	host         string
	port         int
	tls          *tls.Config
	broker       *client.Broker
	httpClient   *http.Client
}

func (c *clientApp) setDefaults() (err error) {
	if c.conf.Name == "" {
		err = fmt.Errorf("source name missing from configuration")
		return
	}
	if c.conf.Target == nil {
		err = fmt.Errorf("target missing from configuration")
		return
	}
	if c.conf.Target.Host == "" {
		err = fmt.Errorf("target host missing from configuration")
		return
	}
	if c.host, c.port, err = c.conf.Target.ParseHost(); err != nil {
		return
	}
	if c.tls, err = getTLSConf(c.conf.Target); err != nil {
		return
	}
	if http.ParseProtocol(c.conf.Target.Protocol) == http.ProtocolKCPHTTP {
		if c.conf.Target.KCP == nil {
			return fmt.Errorf("kcp configuration is required for kcp-http protocol")
		}
		if c.conf.Target.KCP.Host == "" {
			c.conf.Target.KCP.Host = c.host
		}
		c.conf.Target.KCP.ApplyDefaultPort(c.port)
		if err = c.conf.Target.KCP.Validate(); err != nil {
			return fmt.Errorf("invalid target KCP configuration: %w", err)
		}
	}
	if c.conf.BinSize == 0 {
		c.conf.BinSize = 10 * 1024 * 1024 * 1024
	}
	if c.conf.Timeout == 0 {
		c.conf.Timeout = time.Hour * 1
	}
	if c.conf.PollAttempts == 0 {
		c.conf.PollAttempts = 10
	}
	if c.conf.PollInterval == 0 {
		c.conf.PollInterval = 60
	}
	if c.conf.PollDelay == 0 {
		c.conf.PollDelay = 5
	}
	if c.conf.PollMaxCount == 0 {
		c.conf.PollMaxCount = 1000
	}
	if c.conf.StatInterval == 0 {
		c.conf.StatInterval = time.Minute * 5
	}
	if c.conf.CacheAge == 0 {
		c.conf.CacheAge = time.Hour * 24
	}
	if c.conf.ScanDelay == 0 {
		c.conf.ScanDelay = time.Second * 30
	}
	if c.conf.ErrorBackoff == 0 {
		c.conf.ErrorBackoff = 1
	}
	if c.conf.GroupBy == nil || c.conf.GroupBy.String() == "" {
		// Default is up to the first dot of the relative path.
		c.conf.GroupBy = regexp.MustCompile(`^([^\.]*)`)
	}
	var defaultTag *sts.TagConf
	for _, tag := range c.conf.Tags {
		if tag.Pattern == nil {
			defaultTag = tag
			break
		}
		if tag.Method == "" {
			tag.Method = sts.MethodHTTP
		}
	}
	if defaultTag == nil {
		// If no default tag exists, add one
		c.conf.Tags = append(c.conf.Tags, &sts.TagConf{
			Method: sts.MethodHTTP,
		})
	}

	// Set QUIC configuration defaults
	if c.conf.Target.QUIC == nil {
		c.conf.Target.QUIC = &sts.QUICConf{}
	}
	quicConf := c.conf.Target.QUIC

	// Max streams: scale with concurrency (2x threads, minimum 100)
	if quicConf.MaxStreams == 0 {
		threadsDefault := int64(c.conf.Threads * 2)
		if threadsDefault > http.DefaultQUICMaxStreams {
			quicConf.MaxStreams = threadsDefault
		} else {
			quicConf.MaxStreams = http.DefaultQUICMaxStreams
		}
	}

	// Max idle timeout: scale with network latency (timeout/10, minimum 30s)
	if quicConf.MaxIdleTimeout == 0 {
		timeoutBased := c.conf.Timeout / 10
		if timeoutBased > http.DefaultQUICMaxIdleTimeout {
			quicConf.MaxIdleTimeout = timeoutBased
		} else {
			quicConf.MaxIdleTimeout = http.DefaultQUICMaxIdleTimeout
		}
	}

	// Keep-alive: half of idle timeout (minimum 15s)
	if quicConf.KeepAlive == 0 {
		quicConf.KeepAlive = quicConf.MaxIdleTimeout / 2
		if quicConf.KeepAlive < http.DefaultQUICKeepAlive {
			quicConf.KeepAlive = http.DefaultQUICKeepAlive
		}
	}

	// Stream receive window: default 6MB
	if quicConf.MaxStreamReceiveWindow == 0 {
		quicConf.MaxStreamReceiveWindow = http.DefaultQUICMaxStreamReceiveWindow
	}

	// Connection receive window: default 15MB
	if quicConf.MaxConnectionReceiveWindow == 0 {
		quicConf.MaxConnectionReceiveWindow = http.DefaultQUICMaxConnectionReceiveWindow
	}

	return
}

func (c *clientApp) init() (err error) {
	if err = c.setDefaults(); err != nil {
		return
	}

	// Configure the file store to be scanned
	store := &store.Local{
		Root:           filepath.Clean(c.conf.OutDir),
		MinAge:         c.conf.MinAge,
		IncludeHidden:  c.conf.IncludeHidden,
		Include:        c.conf.Include,
		Ignore:         c.conf.Ignore,
		FollowSymlinks: c.dirOutFollow,
	}
	store.AddStandardIgnore()

	configDump := []string{
		fmt.Sprintf("CONFIG: %s", c.conf.Name),
	}

	dump := func(format string, a ...any) {
		configDump = append(configDump, fmt.Sprintf(" - "+format, a...))
	}

	dump("Root Outgoing Directory: %s", c.conf.OutDir)
	dump("File Minimum Age: %s", c.conf.MinAge.String())
	if c.conf.IncludeHidden {
		dump("Includes Hidden Files: Yes")
	} else {
		dump("Includes Hidden Files: No")
	}
	if c.dirOutFollow {
		dump("Follows Symbolic Links: Yes")
	} else {
		dump("Follows Symbolic Links: No")
	}
	for _, p := range c.conf.Include {
		dump("File Include Pattern: %s", p.String())
	}
	for _, p := range c.conf.Ignore {
		dump("File Exclude Pattern: %s", p.String())
	}

	// Configure the file cache
	cache, err := cache.NewJSON(c.dirCache, c.conf.OutDir, "")
	if err != nil {
		return
	}

	dump("Cache Directory: %s", c.dirCache)

	// Configure automated file renaming
	var fileToNewName sts.Rename
	if len(c.conf.Rename) > 0 {
		extraVars := c.conf.GenMappingVars()
		maps := make([]*fileutil.PathMap, len(c.conf.Rename))
		funcs := fileutil.CombineFuncs(
			fileutil.CreateStringFuncs(),
			fileutil.CreateDateFuncs(),
		)
		for i, r := range c.conf.Rename {
			dump("Rename: %s => %s", r.Pattern.String(), r.Template)
			maps[i] = &fileutil.PathMap{
				Pattern:   r.Pattern,
				Template:  r.Template,
				ExtraVars: extraVars,
				Funcs:     funcs,
			}
		}
		mapper := &fileutil.PathMapper{
			Maps:   maps,
			Logger: log.Get(),
		}
		fileToNewName = func(file sts.File) string {
			return mapper.Translate(file.GetName())
		}
	}

	// Configure sorting queue
	dump("File Grouping Pattern: %s", c.conf.GroupBy.String())
	qtags := make([]*queue.Tag, len(c.conf.Tags))
	for i, t := range c.conf.Tags {
		name := ""
		switch t.Method {
		case sts.MethodHTTP:
		default:
			// Ignore unknown methods. This allows us to extend the
			// configuration for outside uses like disk mode, which is not
			// part of this package
			if t.Pattern != nil {
				store.Ignore = append(store.Ignore, t.Pattern)
			}
		}
		if t.Pattern != nil {
			name = t.Pattern.String()
		}
		if t.Order == "" {
			t.Order = sts.OrderFIFO
		}
		chunkSize := t.ChunkSize
		if chunkSize == 0 {
			chunkSize = c.conf.BinSize
		}
		qtags[i] = &queue.Tag{
			Name:      name,
			Priority:  t.Priority,
			Order:     t.Order,
			ChunkSize: int64(chunkSize),
			LastDelay: t.LastDelay,
		}
		if name == "" {
			name = "DEFAULT"
		}
		if t.Method == sts.MethodHTTP {
			dump("(%s) Priority: %d", name, t.Priority)
			dump("(%s) Send Order: %s", name, strings.ToUpper(t.Order))
			if t.Delete {
				dump("(%s) Delete Local: Yes", name)
				dump("(%s) Delete Delay: %s", name, t.DeleteDelay.String())
			} else {
				dump("(%s) Delete Local: No", name)
			}
			dump("(%s) Last File Delay: %s", name, t.LastDelay.String())
		} else {
			dump("(%s) Method: %s", name, t.Method)
		}
	}

	tagger := func(group string) (tag string) {
		for i, t := range c.conf.Tags {
			if t.Pattern == nil {
				continue
			}
			if qtags[i].Name == group || t.Pattern.MatchString(group) {
				tag = qtags[i].Name
				return
			}
		}
		return
	}

	grouper := func(name string) (group string) {
		m := c.conf.GroupBy.FindStringSubmatch(name)
		if len(m) > 1 {
			group = m[1]
			if group != "" && group != name {
				return
			}
		}
		// If the group matches the name or is empty, use the tag as the group
		group = tagger(name)
		return
	}

	nameToTag := func(name string) (tag string) {
		group := grouper(name)
		tag = tagger(group)
		return
	}
	tags := make([]*client.FileTag, len(qtags))
	for i := 0; i < len(tags); i++ {
		tags[i] = &client.FileTag{
			Name:        qtags[i].Name,
			InOrder:     c.conf.Tags[i].Order != "",
			Delete:      c.conf.Tags[i].Delete,
			DeleteDelay: c.conf.Tags[i].DeleteDelay,
		}
	}

	c.httpClient = &http.Client{
		SourceName:           c.conf.Name,
		TargetHost:           c.host,
		TargetPort:           c.port,
		TargetPrefix:         c.conf.Target.PathPrefix,
		TargetKey:            c.conf.Target.Key,
		Timeout:              c.conf.Timeout,
		Compression:          c.conf.Compression,
		TLS:                  c.tls,
		PartialsDecoder:      stage.ReadCompanions,
		BandwidthLogInterval: c.conf.StatInterval,
		KCPConfig:            c.conf.Target.KCP,
		MaxConnections:       c.conf.Threads,
		// HTTP3/QUIC configuration
		Protocol:  http.ParseProtocol(c.conf.Target.Protocol),
		HTTP3Port: c.conf.Target.HTTP3Port,
		QUICConfig: http.ConfigureQUICForClient(
			c.conf.Target.QUIC.MaxStreams,
			c.conf.Target.QUIC.MaxIdleTimeout,
			c.conf.Target.QUIC.KeepAlive,
			c.conf.Target.QUIC.MaxStreamReceiveWindow,
			c.conf.Target.QUIC.MaxConnectionReceiveWindow,
			c.conf.Target.QUIC.EnableDatagrams,
			c.conf.Target.QUIC.DisablePathMTUDiscovery,
			c.conf.Target.QUIC.Disable0RTT,
		),
	}

	dump("Server Host: %s", c.host)
	dump("Server Port: %d", c.port)
	dump("Server Path: %s", c.conf.Target.PathPrefix)
	dump("Compression: %d", c.conf.Compression)

	// Update protocol reporting
	protocol := "HTTP"
	if c.tls != nil {
		protocol = "HTTPS"
	}
	if c.conf.Target.Protocol != "" {
		switch http.ParseProtocol(c.conf.Target.Protocol) {
		case http.ProtocolHTTP3:
			protocol = "HTTP3/QUIC"
		case http.ProtocolAuto:
			protocol = "HTTP3/QUIC (with HTTPS fallback)"
		case http.ProtocolKCPHTTP:
			protocol = "HTTP/1.1 over KCP/UDP"
		}
	}
	dump("Protocol: %s", protocol)
	if http.ParseProtocol(c.conf.Target.Protocol) == http.ProtocolKCPHTTP {
		dump("KCP Server: %s:%d", c.conf.Target.KCP.Host, c.conf.Target.KCP.Port)
		dump("KCP Tuning: %s", c.conf.Target.KCP.Tuning)
	}

	dump("Max Concurrent Requests: %d", c.conf.Threads)
	dump("Request Size: %s", c.conf.BinSize.String())
	dump("Cache Age: %s", c.conf.CacheAge.String())
	dump("Scan Delay: %s", c.conf.ScanDelay.String())
	dump("Poll Delay: %s", c.conf.PollDelay.String())
	dump("Poll Interval: %s", c.conf.PollInterval.String())
	dump("Poll Attempts: %d", c.conf.PollAttempts)
	dump("Poll Max Count: %d", c.conf.PollMaxCount)
	dump("Log Directory: %s", c.conf.LogDir)
	dump("Error Backoff Multiplier: %f", c.conf.ErrorBackoff)

	// Keeping the config dump as a single log entry means fetching recent
	// "lines" from the log will make sure the entire config is captured
	log.Info(strings.Join(configDump, "\n"))

	c.broker = &client.Broker{
		Conf: &client.Conf{
			Name:         c.conf.Name,
			Store:        store,
			Cache:        cache,
			Queue:        queue.NewTagged(qtags, tagger, grouper),
			Recoverer:    c.httpClient.Recover,
			BuildPayload: payload.NewBin,
			Transmitter:  c.httpClient.Transmit,
			TxRecoverer:  c.httpClient.RecoverTransmission,
			Validator:    c.httpClient.Validate,
			Logger:       log.NewFileIO(c.conf.LogDir, nil, nil, false),
			Renamer:      fileToNewName,
			Tagger:       nameToTag,
			CacheAge:     c.conf.CacheAge,
			ScanDelay:    c.conf.ScanDelay,
			Threads:      c.conf.Threads,
			PayloadSize:  c.conf.BinSize,
			StatPayload:  c.conf.StatPayload,
			StatInterval: c.conf.StatInterval,
			PollDelay:    c.conf.PollDelay,
			PollInterval: c.conf.PollInterval,
			PollAttempts: c.conf.PollAttempts,
			PollMaxCount: c.conf.PollMaxCount,
			Tags:         tags,
			ErrorBackoff: c.conf.ErrorBackoff,
		},
	}
	return
}

func (c *clientApp) destroy() {
	c.httpClient.Destroy()
}

func (c *clientApp) cancelRequests() {
	c.httpClient.CancelRequests()
}
