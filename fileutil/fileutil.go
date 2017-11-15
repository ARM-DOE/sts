package fileutil

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

	"code.arm.gov/dataflow/sts"
)

// LockExt is the file extension added to file names as contents are written.
const LockExt = ".lck"

// BlockSize is the number of bytes read into memory.
const BlockSize = 8192

// InitPath will turn a relative path into absolute (based on root) and make
// sure it exists.
func InitPath(root string, path string, isdir bool) (string, error) {
	var err error
	if !filepath.IsAbs(path) {
		if root == "" {
			return path, fmt.Errorf(
				"Cannot use a relative path with an empty root: %s", path)
		}
		path, err = filepath.Abs(filepath.Join(root, path))
		if err != nil {
			return path, err
		}
	}
	pdir := path
	if !isdir {
		pdir = filepath.Dir(pdir)
	}
	if _, err = os.Stat(pdir); os.IsNotExist(err) {
		if err = os.MkdirAll(pdir, os.ModePerm); err != nil {
			return path, err
		}
	}
	return path, nil
}

// FindLine searches the given file for the provided byte array and returns
// that line if found.
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

// LoadJSON reads the file at specified path and decodes the JSON into the
// specified struct.  The input data struct should be a pointer.
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
	return ReadableMD5(fh)
}

// ReadableMD5 computes the MD5 on a sts.Readable instance
func ReadableMD5(handle sts.Readable) (hash string, err error) {
	h := md5.New()
	if _, err = io.Copy(h, handle); err != nil {
		return
	}
	hash = HashHex(h)
	return
}

// PartialMD5 computes the MD5 of part of a file, specified from start byte to
// end byte.
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

// HashHex calls Sum(nil) on the input hash and formats the result in
// hexadecimal.
func HashHex(h hash.Hash) string {
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Copy copies a file byte by byte from the src path to the dst path.
func Copy(src, dst string) error {
	fpSrc, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fpSrc.Close()
	fpDst, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer fpDst.Close()
	_, err = io.Copy(fpDst, fpSrc)
	if err != nil {
		return err
	}
	return nil
}

// Move moves a file from one path to another.  It attempts to do a rename and,
// if that fails, will instead do a copy followed by a deletion of the
// original.  If the destination file already exists it will be overwritten.
func Move(src, dst string) error {
	var err error
	if err = os.Rename(src, dst+LockExt); err != nil {
		if err = Copy(src, dst+LockExt); err != nil {
			return err
		}
		if err = os.Remove(src); err != nil {
			return err
		}
	}
	if err = os.Rename(dst+LockExt, dst); err != nil {
		return err
	}
	return nil
}

// Readdir is a simple wrapper around File.Readdir that accepts a path argument
// as opposed to a file pointer.
func Readdir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdir(-1)
}

// walk mimics the internal filepath.walk() function except that it keeps track
// of the path relative to the original root separate from the path normalized
// by filepath.EvalSymlinks() and also tracks a history of the latter to avoid
// unintentional infinite loops caused by recursive linking.
func walk(
	path string,
	evalPath string,
	info os.FileInfo,
	walkFn filepath.WalkFunc,
	history map[string]os.FileInfo) error {

	err := walkFn(path, info, nil)
	if err != nil {
		if info.IsDir() && err == filepath.SkipDir {
			return nil
		}
		return err
	}

	if !info.IsDir() {
		return nil
	}

	if history != nil {
		if _, ok := history[evalPath]; ok {
			return nil
		}
		history[evalPath] = info
	}

	nodes, err := Readdir(evalPath)
	if err != nil {
		return walkFn(path, info, err)
	}

	for _, node := range nodes {
		nodePath := filepath.Join(path, node.Name())
		nodeEvalPath := nodePath
		if history != nil {
			nodeEvalPath, err = filepath.EvalSymlinks(nodePath)
			if err != nil {
				continue // Just skip it.
			}
		}
		nodeInfo, err := os.Lstat(nodeEvalPath)
		if err != nil {
			if err = walkFn(nodePath, nodeInfo, err); err != nil &&
				err != filepath.SkipDir {
				return err
			}
		} else {
			err = walk(nodePath, nodeEvalPath, nodeInfo, walkFn, history)
			if err != nil {
				if !nodeInfo.IsDir() || err != filepath.SkipDir {
					return err
				}
			}
		}
	}
	return nil
}

// Walk mimics https://golang.org/pkg/path/filepath/#Walk wih the exceptions
// that 1) it allows for the option to follow symbolic links, and 2) the order
// of the calls to walkFn are NOT deterministic (i.e. no lexical ordering).
func Walk(
	root string,
	walkFn filepath.WalkFunc,
	followSymLinks bool) (err error) {

	path := root
	if followSymLinks {
		path, err = filepath.EvalSymlinks(root)
		if err != nil {
			return
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		err = walkFn(root, info, err)
	} else {
		var history map[string]os.FileInfo
		if followSymLinks {
			history = make(map[string]os.FileInfo)
		}
		err = walk(root, path, info, walkFn, history)
	}
	if err == filepath.SkipDir {
		return nil
	}
	return
}
