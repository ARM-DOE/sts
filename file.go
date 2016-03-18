package main

// ScanFile is the interface for a file as found on disk.
type ScanFile interface {
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
}

// SortFile is the interface for a file as needed for sorting.
// Implements ScanFile.
type SortFile interface {
	GetOrigFile() ScanFile
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
	GetTag() string
	GetNext() SortFile
	GetPrev() SortFile
	SetNext(SortFile)
	SetPrev(SortFile)
	InsertAfter(SortFile)
	InsertBefore(SortFile)
}

// SendFile is the interface for a file as needed for sending.
// Implements ScanFile.
type SendFile interface {
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
	GetHash() string
	GetPrevName() string
	GetStarted() int64 // Expects UnixNano
	GetNextAlloc() (int64, int64)
	GetBytesAlloc() int64
	GetBytesSent() int64
	AddAlloc(int64)
	AddSent(int64)
	TimeMs() int64
	IsSent() bool
}

// RecoverFile is a partial duplicate of SendFile and will be the underlying file reference
// for the sendFile wrapper and these are the functions that should be used to properly fill bins
// with only those parts of the file that haven't already been received.
// Also implements ScanFile in order to be inserted into the pipeline at the sorter.
type RecoverFile interface {
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
	GetHash() string
	GetPrevName() string
	GetNextAlloc() (int64, int64)
	GetBytesAlloc() int64
	GetBytesSent() int64
	AddAlloc(int64)
	AddSent(int64)
	IsSent() bool
}

// PollFile is the interface for a file as needed for polling.
type PollFile interface {
	GetRelPath() string
	GetStarted() int64 // Expects UnixNano
}

// DoneFile is the interface for a file as needed for completion.
type DoneFile interface {
	GetPath() string
	GetRelPath() string
}

// ConfirmFile is used by both the poller and receiver for validating full-file
// transfers.  The members are public for JSON encoding/decoding.
type ConfirmFile struct {
	RelPath string `json:"n"`
	Started int64  `json:"t"` // Expects Unix
}
