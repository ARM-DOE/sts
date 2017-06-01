package sts

import "time"

// FinalStatusService is the interface for determining a file's final status.
type FinalStatusService interface {
	GetFileStatus(source, relPath string, sent time.Time) int
}

// ScanFile is the interface for a file as found on disk.
type ScanFile interface {
	GetPath(bool) string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
	Reset() (bool, error)
}

// SortFile is the interface for a file as needed for sorting.
// Implements ScanFile.
type SortFile interface {
	ScanFile
	Invalidate()
	Validate() (bool, error)
	GetOrigFile() ScanFile
	GetGroup() string
	GetNext() SortFile
	GetPrev() SortFile
	GetPrevReq() SortFile
	SetNext(SortFile)
	SetPrev(SortFile)
	InsertAfter(SortFile)
	InsertBefore(SortFile)
}

// SendFile is the interface for a file as needed for sending.
// Implements ScanFile.
type SendFile interface {
	ScanFile
	GetHash() string
	GetPrevName() string
	SetStarted(time.Time)
	GetStarted() time.Time
	SetCompleted(time.Time)
	GetCompleted() time.Time
	GetNextAlloc() (int64, int64)
	GetBytesAlloc() int64
	GetBytesSent() int64
	AddAlloc(int64)
	AddSent(int64) bool // Returns IsSent() to keep the transaction atomic
	TimeMs() int64
	IsSent() bool
	SetCancel(bool)
	GetCancel() bool
	Stat() (bool, error)
}

// RecoverFile is a partial duplicate of SendFile and will be the underlying file reference
// for the sendFile wrapper and these are the functions that should be used to properly fill bins
// with only those parts of the file that haven't already been received.
// Also implements ScanFile in order to be inserted into the pipeline at the sorter.
type RecoverFile interface {
	ScanFile
	GetHash() string
	GetNextAlloc() (int64, int64)
	GetBytesAlloc() int64
	GetBytesSent() int64
	AddAlloc(int64)
	AddSent(int64)
	IsSent() bool
	Stat() (bool, error)
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
	ScanFile
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
