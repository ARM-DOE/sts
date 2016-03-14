package main

// ScanFile is the interface for a file as found on disk.
type ScanFile interface {
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
}

// SortFile is the interface for a file as needed for sorting.
type SortFile interface {
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
	GetTag() string
	GetNext() SortFile
	GetPrev() SortFile
	SetNext(SortFile)
	SetPrev(SortFile)
	GetNextByTag() SortFile
	GetPrevByTag() SortFile
	SetNextByTag(SortFile)
	SetPrevByTag(SortFile)
	InsertAfter(SortFile)
	InsertAfterByTag(SortFile)
	InsertBefore(SortFile)
	InsertBeforeByTag(SortFile)
}

// SendFile is the interface for a file as needed for sending.
type SendFile interface {
	GetSortFile() SortFile
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
	GetHash() string
	GetPrevName() string
	GetStarted() int64
	GetBytesAlloc() int64
	GetBytesSent() int64
	AddAlloc(int64)
	AddSent(int64)
	Time() int64
	IsSent() bool
}

// DoneFile is the interface for a file as needed for completion.
type DoneFile interface {
	GetSortFile() SortFile
}

// ConfirmFile is used by both the poller and receiver for validating full-file
// transfers.  The members are public for JSON encoding/decoding.
type ConfirmFile struct {
	RelPath string
	Started int64
}
