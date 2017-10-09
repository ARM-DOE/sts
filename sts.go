package sts

import (
	"io"
	"time"
)

// Logger is the generic logging interface
type Logger interface {
	Debug(...interface{})
	Info(...interface{})
	Error(...interface{})
}

// SendLogger is the interface for logging on the sending side
type SendLogger interface {
	Sent(Sent)
	WasSent(relPath string, after time.Time, before time.Time) bool
}

// ReceiveLogger is the interface for logging on the incoming side
type ReceiveLogger interface {
	Received(source string, file Received)
	WasReceived(
		source string,
		relPath string,
		after time.Time,
		before time.Time) bool
}

// FileSource is the interface for reading and deleting from a store of file
// objects on the client (i.e. sending) side
type FileSource interface {
	Scan(func(string) File) ([]File, time.Time, error)
	Remove(File) error
	Sync(File) (File, error)
	GetOpener() Open
}

// FileCache is the interface for caching a collection of files
type FileCache interface {
	Iterate(func(Cached) bool)
	Get(string) Cached
	Add(Hashed)
	Done(string)
	Remove(string)
	Persist() error
}

// FileQueue is the interface for getting files in the proper order
type FileQueue interface {
	Add([]Hashed)
	GetNext() Sendable
	Queued(string) bool
}

// GateKeeper is the interface for managing the "putting away" of files
// received on the server
type GateKeeper interface {
	Recover() error
	Scan(source string) ([]*Partial, error)
	Receive(*Partial, io.Reader) error
	GetFileStatus(source, relPath string, sent time.Time) int
}

// Recover is the function type for determining what partial files need to be
// sent since previous run
type Recover func() ([]*Partial, error)

// Open is a generic function for creating a Readable handle to a file
type Open func(File) (Readable, error)

// PayloadFactory creates a Payload instance based on the input max size (in
// bytes) and Open function
type PayloadFactory func(int64, Open) Payload

// Transmit is the function type for actually sending payload over the wire (or
// whatever).  It returns some representation of how much was successfully
// transmitted and any error encountered.
type Transmit func(Payload) (int, error)

// PayloadDecoderFactory creates a decoder instance to be used to parse a
// multipart byte stream with the metadata at the beginning
type PayloadDecoderFactory func(metaLen int, payload io.Reader) (PayloadDecoder, error)

// Validate is the function type for validating files sent by the client were
// successfully received by the server
type Validate func([]Pollable) ([]Polled, error)

// Translate is a general function type for converting one string to another
type Translate func(string) string

// File is the most basic interface for a File object
type File interface {
	GetPath() string
	GetRelPath() string
	GetSize() int64
	GetTime() int64
}

// Readable is a wrapper for being able to seek, read, and close a file
// generically
type Readable interface {
	io.Reader
	io.Seeker
	io.Closer
}

// Hashed is the interface a file must implement to include a signature
type Hashed interface {
	File
	GetHash() string
}

// Cached is the interface a file must implement to be cached
type Cached interface {
	Hashed
	IsDone() bool
}

// Recovered ...
type Recovered interface {
	Hashed
	GetPrev() string
	Allocate(int64) (int64, int64)
	IsAllocated() bool
}

// Sendable is the interface a file must implement to be sent by the client
type Sendable interface {
	Hashed
	GetPrev() string
	GetSlice() (int64, int64)
}

// Binnable is the interface a file must implement to have a chunk be part of
// a sendable bin
type Binnable interface {
	Sendable
	GetNextAlloc() (int64, int64)
	AddAlloc(int64)
	IsAllocated() bool
}

// Binned is the interface for a single file chunk that is part of a payload
type Binned interface {
	GetName() string
	GetPrev() string
	GetFileHash() string
	GetFileSize() int64
	GetSlice() (int64, int64)
}

// Payload is the interface to a slice of Binnables that can be read for
// transmission
type Payload interface {
	Add(Binnable) bool
	Remove(Binned)
	IsFull() bool
	Split(nParts int) Payload
	GetSize() int64
	GetParts() []Binned
	EncodeHeader() ([]byte, error)
	GetEncoder() io.Reader
	GetStarted() time.Time
	GetCompleted() time.Time
}

// PayloadDecoder is the interface on the receiving side to a sent payload
type PayloadDecoder interface {
	GetParts() []Binned
	Next() (io.Reader, bool)
}

// Partial ...
type Partial struct {
	Name   string       `json:"path"`
	Prev   string       `json:"prev"`
	Size   int64        `json:"size"`
	Hash   string       `json:"hash"`
	Source string       `json:"src"`
	Parts  []*ByteRange `json:"parts"`
}

// ByteRange ...
type ByteRange struct {
	Beg int64 `json:"b"`
	End int64 `json:"e"`
}

// Sent is the interface a file must implement to be recorded as sent
type Sent interface {
	GetName() string
	GetSize() int64
	GetHash() string
	TimeMs() int64
}

// Pollable is the interface a file must implement to be polled by the client
// for whether or not the server received the file successfully
type Pollable interface {
	GetName() string
	GetStarted() time.Time
}

// Polled is the interface a file must implement as a response to being polled
type Polled interface {
	GetName() string
	NotFound() bool
	Waiting() bool
	Failed() bool
	Received() bool
}

// Received ...
type Received interface {
	GetName() string
	GetSize() int64
	GetHash() string
}

const (
	// ConfirmNone is the indicator that a file has not been confirmed.
	ConfirmNone = 0

	// ConfirmFailed is the indicator that file confirmation failed.
	ConfirmFailed = 1

	// ConfirmPassed is the indicator that file confirmation succeeded.
	ConfirmPassed = 2

	// ConfirmWaiting is the indicator that file confirmation succeeded but its
	// predecessor has not been confirmed.
	ConfirmWaiting = 3
)
