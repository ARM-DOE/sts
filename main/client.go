package main

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/cache"
	"code.arm.gov/dataflow/sts/client"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/payload"
	"code.arm.gov/dataflow/sts/queue"
	"code.arm.gov/dataflow/sts/stage"
	"code.arm.gov/dataflow/sts/store"
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
	if c.conf.StatInterval == 0 {
		c.conf.StatInterval = time.Minute * 5
	}
	if c.conf.CacheAge == 0 {
		c.conf.CacheAge = c.conf.StatInterval
	}
	if c.conf.ScanDelay == 0 {
		c.conf.ScanDelay = time.Second * 30
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
	return
}

func (c *clientApp) init() (err error) {
	if err = c.setDefaults(); err != nil {
		return
	}

	// Configure the file store to be scanned
	store := &store.Local{
		Root:           c.conf.OutDir,
		MinAge:         c.conf.MinAge,
		IncludeHidden:  c.conf.IncludeHidden,
		Include:        c.conf.Include,
		Ignore:         c.conf.Ignore,
		FollowSymlinks: c.dirOutFollow,
	}
	store.AddStandardIgnore()
	pfx := "CONFIG: Store =>"
	log.Debug(pfx, "Root Outgoing Directory:", c.conf.OutDir)
	log.Debug(pfx, "File Minimum Age:", c.conf.MinAge.String())
	if c.conf.IncludeHidden {
		log.Debug(pfx, "Includes Hidden Files")
	} else {
		log.Debug(pfx, "Excludes Hidden Files")
	}
	if c.dirOutFollow {
		log.Debug(pfx, "Follows Symbolic Links")
	} else {
		log.Debug(pfx, "Does NOT Follow Symbolic Links")
	}
	for _, p := range c.conf.Include {
		log.Debug(pfx, "Has File Include Pattern:", p.String())
	}
	for _, p := range c.conf.Ignore {
		log.Debug(pfx, "Has File Exclude Pattern:", p.String())
	}

	// Configure the file cache
	cache, err := cache.NewJSON(c.dirCache, c.conf.OutDir, "")
	if err != nil {
		return
	}
	pfx = "CONFIG: Cache =>"
	log.Debug(pfx, "Directory:", c.dirCache)

	// Configure automated file renaming
	var fileToNewName sts.Rename
	if len(c.conf.Rename) > 0 {
		pfx = "Renaming =>"
		extraVars := map[string]string{
			"__source": c.conf.Name,
		}
		maps := make([]*fileutil.PathMap, len(c.conf.Rename))
		for i, r := range c.conf.Rename {
			log.Debug(pfx, "Map From:", r.Pattern.String(), "To:", r.Template)
			maps[i] = &fileutil.PathMap{
				Pattern:   r.Pattern,
				Template:  r.Template,
				ExtraVars: extraVars,
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
	pfx = "CONFIG: Group =>"
	log.Debug(pfx, "Pattern:", c.conf.GroupBy.String())
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
		qtags[i] = &queue.Tag{
			Name:      name,
			Priority:  t.Priority,
			Order:     t.Order,
			ChunkSize: int64(c.conf.BinSize),
			LastDelay: t.LastDelay,
		}
		if name == "" {
			name = "DEFAULT"
		}
		log.Debug(pfx, name, "=>", "Priority:", t.Priority)
		log.Debug(pfx, name, "=>", "Order:", strings.ToUpper(t.Order))
		log.Debug(pfx, name, "=>", "Delay:", t.LastDelay)
	}

	grouper := func(name string) (group string) {
		m := c.conf.GroupBy.FindStringSubmatch(name)
		if len(m) == 0 {
			return
		}
		group = m[1]
		return
	}
	tagger := func(group string) (tag string) {
		for i, t := range c.conf.Tags {
			if t.Pattern == nil {
				continue
			}
			if t.Pattern.MatchString(group) {
				tag = qtags[i].Name
				return
			}
		}
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

	httpClient := &http.Client{
		SourceName:      c.conf.Name,
		TargetHost:      c.host,
		TargetPort:      c.port,
		TargetPrefix:    c.conf.Target.PathPrefix,
		TargetKey:       c.conf.Target.Key,
		Timeout:         c.conf.Timeout,
		Compression:     c.conf.Compression,
		TLS:             c.tls,
		PartialsDecoder: stage.ReadCompanions,
	}
	pfx = "CONFIG: HTTP Client =>"
	log.Debug(pfx, "Name:", c.conf.Name)
	log.Debug(pfx, "Target Host:", c.host)
	log.Debug(pfx, "Target Port:", c.port)
	log.Debug(pfx, "Target Path:", c.conf.Target.PathPrefix)
	log.Debug(pfx, "Compression:", c.conf.Compression)
	if c.tls == nil {
		log.Debug(pfx, "TLS: Yes")
	} else {
		log.Debug(pfx, "TLS: No")
	}

	pfx = "CONFIG: Client =>"
	log.Debug(pfx, "Max Concurrent Requests:", c.conf.Threads)
	log.Debug(pfx, "Request Size:", c.conf.BinSize.String())
	log.Debug(pfx, "Cache Age:", c.conf.CacheAge.String())
	log.Debug(pfx, "Scan Delay:", c.conf.ScanDelay.String())
	log.Debug(pfx, "Poll Delay:", c.conf.PollDelay.String())
	log.Debug(pfx, "Poll Interval:", c.conf.PollInterval.String())
	log.Debug(pfx, "Logging Directory:", c.conf.LogDir)

	c.broker = &client.Broker{
		Conf: &client.Conf{
			Store:        store,
			Cache:        cache,
			Queue:        queue.NewTagged(qtags, tagger, grouper),
			Recoverer:    httpClient.Recover,
			BuildPayload: payload.NewBin,
			Transmitter:  httpClient.Transmit,
			TxRecoverer:  httpClient.RecoverTransmission,
			Validator:    httpClient.Validate,
			Logger:       log.NewSend(c.conf.LogDir, nil, nil),
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
			Tags:         tags,
		},
	}
	return
}
