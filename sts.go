package sts

import (
	"crypto/tls"
	"time"

	"github.com/alecthomas/units"
)

// SenderConf contains all parameters necessary for the sending daemon.
type SendConf struct {
	Threads      int
	Compression  int
	SourceName   string
	TargetName   string
	TargetHost   string
	TargetKey    string
	TLS          *tls.Config
	BinSize      units.Base2Bytes
	Timeout      time.Duration
	StatInterval time.Duration
	PollInterval time.Duration
	PollDelay    time.Duration
	PollAttempts int
}

// Protocol returns the protocl (http vs https) based on the configuration.
func (sc *SendConf) Protocol() string {
	if sc.TLS != nil {
		return "https"
	}
	return "http"
}

// FinalStatusService is the interface for determining a file's final status.
type FinalStatusService interface {
	GetFileStatus(source, relPath string, sent time.Time) int
}

type File interface {
	GetPath(bool) string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
}

type Sortable interface {
	File
	GetHash() string
}

type Sendable interface {
	Sortable
	GetPrevName() string
	GetSlice() (int64, int64)
}

type Binnable interface {
	Sendable
	GetNextAlloc() (int64, int64)
	AddAlloc(int64)
}

type Sent interface {
	Sendable
	TimeMs() int64
}

type Pollable interface {
	GetRelPath() string
	GetStarted() time.Time
}

type Polled interface {
	GetRelPath()
	IsDone() bool
}

// RecoverFile is a partial duplicate of SendFile and will be the underlying
// file reference for the sendFile wrapper and these are the functions that
// should be used to properly fill bins with only those parts of the file that
// haven't already been received.
// Also implements ScanFile in order to be inserted into the pipeline at the
// sorter.
type RecoverFile interface {
	File
	GetNextAlloc() (int64, int64)
	GetBytesAlloc() int64
	GetBytesSent() int64
	AddAlloc(int64)
	AddSent(int64)
	IsSent() bool
	// Stat() (bool, error)
}

// Companion is the interface to a companion file
type Companion interface {
	AddPart(partHash string, beg int64, end int64)
	Write() error
	Delete() error
	IsComplete() bool
}

// RecvFile is the interface for a received file
type RecvFile interface {
	File
	GetCompanion() Companion
}

// PollFile is the interface for a file as needed for polling.
type PollFile interface {
	GetOrigFile() interface{} // Needs to be generic to accommodate polling at different stages.
	GetRelPath() string
	GetStarted() time.Time
}

// DoneFile is the interface for a file as needed for completion.
type DoneFile interface {
	GetPath() string
	GetRelPath() string
	GetSuccess() bool
}

// ConfirmFile is used by both the poller and receiver for validating full-file
// transfers.  The members are public for JSON encoding/decoding.
type ConfirmFile struct {
	RelPath string `json:"n"`
	Started int64  `json:"t"` // Expects Unix
}
