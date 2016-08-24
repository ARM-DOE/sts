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
	"path/filepath"
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

// GuessCompressed attempts to determine if a file is compressed.
func GuessCompressed(path string) bool {
	return filepath.Ext(path) == ".gz"
}

// WriteJSON writes the input data in JSON format to the specified path.
func WriteJSON(path string, data interface{}) (err error) {
	var jsonBytes []byte
	if jsonBytes, err = json.Marshal(data); err != nil {
		return
	}
	if err = ioutil.WriteFile(path+LockExt, jsonBytes, 0644); err != nil {
		return
	}
	err = os.Rename(path+LockExt, path)
	return
}

// LoadJSON reads the file at specified path and decodes the JSON into the specified
// struct.  The input data struct should be a pointer.
func LoadJSON(path string, data interface{}) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	fromJSON := json.NewDecoder(fh)
	err = fromJSON.Decode(data)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

// StringMD5 computes the MD5 hash from an array of bytes.
func StringMD5(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return HashHex(h)
}

// FileMD5 computes the MD5 of a file given a path.
func FileMD5(path string) (hash string, err error) {
	var fh *os.File
	if fh, err = os.Open(path); err != nil {
		return
	}
	defer fh.Close()
	h := md5.New()
	if _, err = io.Copy(h, fh); err != nil {
		return
	}
	hash = HashHex(h)
	return
}

// PartialMD5 computes the MD5 of part of a file, specified from start byte to end byte.
func PartialMD5(path string, start int64, end int64) (hash string, err error) {
	var fh *os.File
	if fh, err = os.Open(path); err != nil {
		return
	}
	defer fh.Close()
	fh.Seek(start, 0)
	h := md5.New()
	if _, err = io.CopyN(h, fh, end-start); err != nil {
		return
	}
	hash = HashHex(h)
	return
}

// HashHex calls Sum(nil) on the input hash and formats the result in hexadecimal.
func HashHex(h hash.Hash) string {
	return fmt.Sprintf("%x", h.Sum(nil))
}
