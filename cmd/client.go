package main

import (
	"crypto/tls"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"code.arm.gov/dataflow/sts/cache"
	"code.arm.gov/dataflow/sts/client"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/http"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/payload"
	"code.arm.gov/dataflow/sts/queue"
	"code.arm.gov/dataflow/sts/store"
)

type clientApp struct {
	root   string
	outDir string
	dirs   *clientDirs
	conf   *sourceConf
	host   string
	port   int
	tls    *tls.Config
	logDir string
	broker *client.Broker
}

func (c *clientApp) setDefaults() (err error) {
	if c.conf.Name == "" {
		err = fmt.Errorf("Source name missing from configuration")
		return
	}
	if c.conf.Target == nil {
		err = fmt.Errorf("Target missing from configuration")
		return
	}
	if c.conf.Target.Host == "" {
		err = fmt.Errorf("Target host missing from configuration")
		return
	}
	c.host = c.conf.Target.Host
	c.port = 1992
	hostParts := strings.Split(c.host, ":")
	if len(hostParts) == 2 {
		c.host = hostParts[0]
		if c.port, err = strconv.Atoi(string(c.host[1])); err != nil {
			err = fmt.Errorf("Cannot parse port: %s", c.host)
			return
		}
	}
	if c.conf.Target.Name == "" {
		c.conf.Target.Name = c.host
	}
	if c.conf.Target.TLSCertPath != "" {
		var certPath string
		if certPath, err = fileutil.InitPath(c.root, c.conf.Target.TLSCertPath, false); err != nil {
			return
		}
		if c.tls, err = http.GetTLSConf("", "", certPath); err != nil {
			return
		}
	}
	if c.dirs.Cache == "" {
		c.dirs.Cache = ".sts"
	}
	if c.dirs.Logs == "" {
		c.dirs.Logs = "logs"
	}
	if c.dirs.Out == "" {
		c.dirs.Out = "out"
	}
	c.logDir = filepath.Join(c.dirs.Logs, c.dirs.LogsOut)
	if c.logDir, err = fileutil.InitPath(c.root, c.logDir, true); err != nil {
		return
	}
	if c.conf.BinSize == 0 {
		c.conf.BinSize = 10 * 1024 * 1024 * 1024
	}
	if c.conf.Timeout == 0 {
		c.conf.Timeout = 3600 * 60
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
		c.conf.GroupBy = regexp.MustCompile(`^([^\.]*)`) // Default is up to the first dot of the relative path.
	}
	var defaultTag *tagConf
	for _, tag := range c.conf.Tags {
		if tag.Pattern == nil {
			defaultTag = tag
			break
		}
	}
	if defaultTag == nil {
		// If no default tag exists, add one
		c.conf.Tags = append(c.conf.Tags, &tagConf{
			Method: methodHTTP,
		})
	}
	return
}

func (c *clientApp) init() (err error) {
	if err = c.setDefaults(); err != nil {
		return
	}
	c.outDir = c.conf.OutDir
	if c.outDir != "" {
		if c.outDir, err = fileutil.InitPath(c.root, c.outDir, true); err != nil {
			return
		}
	} else if c.outDir, err = fileutil.InitPath(c.root, filepath.Join(c.dirs.Out, c.conf.Target.Name), true); err != nil {
		return
	}
	store := &store.Local{
		Root:           c.outDir,
		MinAge:         c.conf.MinAge,
		Include:        c.conf.Include,
		Ignore:         c.conf.Ignore,
		FollowSymlinks: c.dirs.OutFollow,
	}
	cache, err := cache.NewJSON(c.outDir, "")
	if err != nil {
		return
	}
	qtags := make([]*queue.Tag, len(c.conf.Tags))
	for i, t := range c.conf.Tags {
		name := ""
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
	}
	grouper := func(name string) (group string) {
		m := c.conf.GroupBy.FindStringSubmatch(name)
		if len(m) == 0 {
			return
		}
		group = m[1]
		return
	}
	tagger := func(name string) (tag string) {
		group := grouper(name)
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
	tags := make([]*client.FileTag, len(qtags))
	for i := 0; i < len(tags); i++ {
		tags[i] = &client.FileTag{
			Name:    qtags[i].Name,
			InOrder: c.conf.Tags[i].Order != "",
		}
	}
	httpClient := &http.Client{
		SourceName:  c.conf.Name,
		TargetHost:  c.host,
		TargetPort:  c.port,
		TargetKey:   c.conf.Target.Key,
		Timeout:     c.conf.Timeout,
		Compression: c.conf.Compression,
		TLS:         c.tls,
	}
	c.broker = &client.Broker{
		Conf: &client.Conf{
			Store:        store,
			Cache:        cache,
			Queue:        queue.NewTagged(qtags, tagger, grouper),
			Recoverer:    httpClient.Recover,
			BuildPayload: payload.NewBin,
			Transmitter:  httpClient.Transmit,
			Validator:    httpClient.Validate,
			Logger:       log.NewSend(c.logDir, c.conf.Target.Name),
			Tagger:       tagger,
			CacheAge:     c.conf.CacheAge,
			ScanDelay:    c.conf.ScanDelay,
			Threads:      c.conf.Threads,
			PayloadSize:  c.conf.BinSize,
			StatInterval: c.conf.StatInterval,
			PollDelay:    c.conf.PollDelay,
			PollInterval: c.conf.PollInterval,
			Tags:         tags,
		},
	}
	return
}
