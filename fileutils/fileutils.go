package fileutils

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
)

// LockExt is the file extension added to file names as contents are written.
const LockExt = ".lck"

// BlockSize is the number of bytes read into memory.
const BlockSize = 8192

// FindLine searches the given file for the provided byte array and returns that
// line if found.
func FindLine(path string, b []byte) string {
	fh, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer fh.Close()
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		fb := scanner.Bytes()
		if bytes.Contains(fb, b) {
			return string(fb)
		}
	}
	return ""
}

// WriteJSON writes the input data in JSON format to the specified path.
func WriteJSON(path string, data interface{}) error {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	ioutil.WriteFile(path+LockExt, jsonBytes, 0644)
	os.Rename(path+LockExt, path)
	return nil
}

// LoadJSON reads the file at specified path and decodes the JSON into the specified
// struct.  The input data struct should be a pointer.
func LoadJSON(path string, data interface{}) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	fromJSON := json.NewDecoder(fh)
	err = fromJSON.Decode(data)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

// StringMD5 computes the MD5 hash from an array of bytes.
func StringMD5(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// FileMD5 computes the MD5 of a file given a path.
func FileMD5(path string) (string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf(fmt.Sprintf("MD5 failed because file not found: %s", path))
	}
	stream := NewMD5()
	stream.Read(fh)
	return stream.Sum(), nil
}

// PartialMD5 computes the MD5 of part of a file, specified from start byte to end byte.
func PartialMD5(path string, start int64, end int64) (string, error) {
	fh, err := os.Open(path)
	defer fh.Close()
	if err != nil {
		return "", err
	}
	fh.Seek(start, 0)
	hash := NewMD5()
	buff := make([]byte, hash.BlockSize)
	nreads := int((end-start)/BlockSize) + 1
	for i := 0; i < nreads; i++ {
		if i == nreads-1 {
			buff = make([]byte, (end-start)%int64(hash.BlockSize))
		}
		fh.Read(buff)
		hash.Update(buff)
	}
	return hash.Sum(), nil
}

// MD5 is the struct for streaming an MD5 checksum.
type MD5 struct {
	Hash      hash.Hash
	BlockSize int
	Bytes     int64
}

// NewMD5 returns a default MD5 reference.
func NewMD5() *MD5 {
	hash := &MD5{}
	hash.Hash = md5.New()
	hash.BlockSize = BlockSize
	return hash
}

// Update updates the checksum based on the input bytes.
func (hash *MD5) Update(addedBytes []byte) {
	hash.Hash.Write(addedBytes)
	hash.Bytes += int64(len(addedBytes))
}

// Sum formats the checksum as a string.
func (hash *MD5) Sum() string {
	return fmt.Sprintf("%x", hash.Hash.Sum(nil))
}

// Read iterates over an io.Reader until EOF, computing the checksum along the way.
func (hash *MD5) Read(inStream io.Reader) string {
	byteBlock := make([]byte, hash.BlockSize)
	for {
		bytesRead, eof := inStream.Read(byteBlock)
		hash.Update(byteBlock[0:bytesRead])
		if eof != nil {
			return hash.Sum()
		}
	}
}
