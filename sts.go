package sts

import (
	"io"
	"sync"
	"time"
)

const (
	// DefLog is the default logs directory name
	DefLog = "logs"

	// DefLogMsg is the default log messages directory name
	// (appended to "logs")
	DefLogMsg = "messages"

	// DefLogOut is the default outgoing log messages directory name
	// (appended to "logs")
	DefLogOut = "outgoing_to"

	// DefLogIn is the default incoming log messages directory name
	// (appended to "logs")
	DefLogIn = "incoming_from"

	// DefOut is the default outgoing data directory name
	DefOut = "outgoing_to"

	// DefCache is the default cache directory name
	DefCache = ".sts"

	// DefStage is the default stage directory name
	DefStage = "stage"

	// DefFinal is the default final directory name
	DefFinal = "incoming_from"

	// MethodHTTP indicates HTTP transfer method
	MethodHTTP = "http"

	// DefaultPort is the default TCP port used for HTTP communication
	DefaultPort = 1992
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
	WasSent(name string, after time.Time, before time.Time) bool
}

// ReceiveLogger is the interface for logging on the incoming side
type ReceiveLogger interface {
	Parse(
		handler func(name, renamed, hash string, size int64, t time.Time) bool,
		after time.Time,
		before time.Time) bool
	Received(file Received)
	WasReceived(
		name string,
		after time.Time,
		before time.Time) bool
}

// FileSource is the interface for reading and deleting from a store of file
// objects on the client (i.e. sending) side
type FileSource interface {
	Scan(func(string) File) ([]File, time.Time, error)
	GetOpener() Open
	Remove(File) error
	Sync(File) (File, error)
	IsNotExist(error) bool
}

// FileCache is the interface for caching a collection of files
type FileCache interface {
	Boundary() time.Time
	Iterate(func(Cached) bool)
	Get(string) Cached
	Add(Hashed)
	Done(name string, whileLocked func(Cached))
	Reset(string)
	Remove(string)
	Persist(boundary time.Time) error
}

// FileQueue is the interface for getting files in the proper order
type FileQueue interface {
	Push([]Hashed)
	Pop() Sendable
}

// Dispatcher is the interface for broadcasting messages
type Dispatcher interface {
	Send(string) error
}

// GateKeeper is the interface for managing the "putting away" of files
// received on the server
type GateKeeper interface {
	Recover() error
	Scan(version string) ([]byte, error)
	Prepare(request []Binned)
	Receive(*Partial, io.Reader) error
	Received([]Binned) (nRecvd int)
	GetFileStatus(relPath string, sent time.Time) int
	Stop(*sync.WaitGroup)
}

// GateKeeperFactory creates GateKeeper instances
type GateKeeperFactory func(source string) GateKeeper

// ClientStatus uses bitmasking to represent the status of a client
type ClientStatus uint

const (
	// ClientIsDisabled indicates that transfer is to not occur
	ClientIsDisabled ClientStatus = 1 << iota

	// ClientIsApproved indicates that transfer should be enabled
	ClientIsApproved

	// ClientHasUpdatedConfiguration indicates, well, the updated configuration
	// should be retrieved
	ClientHasUpdatedConfiguration
)

// ClientManager is responsible for disseminating and collecting information
// about a client (sender)
type ClientManager interface {
	GetClientStatus(clientID, clientName, clientOS string) (ClientStatus, error)
	GetClientConf(clientID string) (*ClientConf, error)
	SetClientConfReceived(clientID string, when time.Time) error
}

// EncodeClientID creates a decodable composite ID from a key and unique ID
type EncodeClientID func(key, uid string) (clientID string)

// DecodeClientID converts a composite client ID into its original key and
// unique ID
type DecodeClientID func(clientID string) (key, uid string)

// RequestValidator validates an incoming request
type RequestValidator func(source, key string) bool

// DecodePartials decodes the input reader into a slice of Partial instance
// pointers
type DecodePartials func(r io.Reader) ([]*Partial, error)

// Recover is the function type for determining what partial files need to be
// sent since previous run
type Recover func() ([]*Partial, error)

// Open is a generic function for creating a Readable handle to a file
type Open func(File) (Readable, error)

// Rename is a generic function for generating a new name for a file
type Rename func(File) string

// PayloadFactory creates a Payload instance based on the input max size (in
// bytes) and Open function
type PayloadFactory func(int64, Open, Rename) Payload

// Transmit is the function type for actually sending payload over the wire (or
// whatever).  It returns some representation of how much was successfully
// transmitted and any error encountered.
type Transmit func(Payload) (int, error)

// RecoverTransmission is the function type for querying the server about a
// failed payload to determine which parts if any were successfully received
// in order to avoid duplication
type RecoverTransmission func(Payload) (int, error)

// PayloadDecoderFactory creates a decoder instance to be used to parse a
// multipart byte stream with the metadata at the beginning
type PayloadDecoderFactory func(
	metaLen int, pathSep string, payload io.Reader) (PayloadDecoder, error)

// Validate is the function type for validating files sent by the client were
// successfully received by the server
type Validate func([]Pollable) ([]Polled, error)

// Translate is a general function type for converting one string to another
type Translate func(string) string

// File is the most basic interface for a File object
type File interface {
	GetPath() string
	GetName() string
	GetSize() int64
	GetTime() int64
	GetMeta() []byte
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

// Cached is the interface a cached file must implement
type Cached interface {
	Hashed
	IsDone() bool
}

// Recovered is the interface a file must implement to be recovered after a
// partial send
type Recovered interface {
	Hashed
	GetPrev() string
	GetSendSize() int64
	Allocate(int64) (int64, int64)
	IsAllocated() bool
}

// Sendable is the interface a file must implement to be sent by the client
type Sendable interface {
	Hashed
	GetPrev() string
	GetSlice() (int64, int64)
	GetSendSize() int64
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
	GetRenamed() string
	GetPrev() string
	GetFileTime() int64
	GetFileHash() string
	GetFileSize() int64
	// GetSendSize will almost always be the same as GetFileSize.  The one
	// exception is for files recovered where some portion of the file was
	// sent earlier.
	GetSendSize() int64
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

// Partial is the JSON-encodable struct containing metadata for a single file
// on the receiving end
type Partial struct {
	Name    string       `json:"path"`
	Renamed string       `json:"renamed"`
	Prev    string       `json:"prev"`
	Time    int64        `json:"time"`
	Size    int64        `json:"size"`
	Hash    string       `json:"hash"`
	Source  string       `json:"src"`
	Parts   []*ByteRange `json:"parts"`
}

// ByteRange is the JSON-encodable struct used by the Partial for tracking a
// a single byte range
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
	Sent
	GetPrev() string // Needed in case the file needs to be resent
	GetStarted() time.Time
}

// Polled is the interface a file must implement as a response to being polled
type Polled interface {
	Pollable
	NotFound() bool
	Waiting() bool
	Failed() bool
	Received() bool
}

// Received is the interface a file must implement to be logged as received
type Received interface {
	GetName() string
	GetRenamed() string
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
