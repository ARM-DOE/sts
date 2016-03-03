package util

import (
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
)

// GenerateMD5 generates and returns an md5 string from an array of bytes.
func GenerateMD5(data []byte) string {
	new_hash := md5.New()
	new_hash.Write(data)
	return fmt.Sprintf("%x", new_hash.Sum(nil))
}

// FileMD5 calculates the MD5 of a file given a path.
// Unlike GenerateMD5, FileMD5 doesn't load the whole file into memory.
func FileMD5(path string) (string, error) {
	fi, err := os.Open(path)
	if err != nil {
		return "", errors.New("File " + path + " does not exist, can't generate MD5")
	}
	stream_md5 := NewStreamMD5()
	stream_md5.ReadAll(fi)
	return stream_md5.SumString(), nil
}

// StreamMD5 is a struct that manages the digesting of byte blocks to an instance of an MD5 hash.
type StreamMD5 struct {
	md5        hash.Hash
	BlockSize  int
	bytes_sent int64
}

// NewStreamMD5 creates a new instance of the StreamMD5 struct with a default BlockSize of 8192.
func NewStreamMD5() *StreamMD5 {
	new_stream := &StreamMD5{}
	new_stream.md5 = md5.New()
	new_stream.BlockSize = 8192
	return new_stream
}

// Update(bytes) writes the provided bytes to the internal md5 hash.
func (stream *StreamMD5) Update(added_bytes []byte) {
	stream.md5.Write(added_bytes)
	stream.bytes_sent += int64(len(added_bytes))
}

// SumString returns the sum of the MD5 hash formatted as a string.
func (stream *StreamMD5) SumString() string {
	return fmt.Sprintf("%x", stream.md5.Sum(nil))
}

// ReadAll takes an io.Reader and consumes blocks of size BlockSize from it until EOF.
// When finished, it returns the SumString of the MD5.
func (stream *StreamMD5) ReadAll(read_stream io.Reader) string {
	byte_block := make([]byte, stream.BlockSize)
	for {
		bytes_read, eof := read_stream.Read(byte_block)
		stream.Update(byte_block[0:bytes_read])
		if eof != nil {
			return stream.SumString()
		}
	}
}
