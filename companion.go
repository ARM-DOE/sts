package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/ARM-DOE/sts/fileutils"
)

// CompExt is the extension added to "companion" files on the receiving end.
const CompExt = ".cmp"

var gLock sync.Mutex

// Companion provides the functionality for working with "companion" files.
type Companion struct {
	Path       string
	PrevFile   string
	Size       int64
	SourceName string
	Parts      map[string]*PartData
	Hash       string
}

// PartData holds the information for a particular "part" of a file.  A companion has 1..N parts.
type PartData struct {
	Hash  string
	Start int64
	End   int64
}

// NewCompanion returns a new Companion reference.
func NewCompanion(source string, path string, size int64) (cmp *Companion, err error) {
	gLock.Lock()
	defer gLock.Unlock()
	cmp, err = ReadCompanion(path)
	if cmp != nil && err == nil {
		return
	}
	cmp = &Companion{}
	cmp.Path = path
	cmp.Size = size
	cmp.Parts = make(map[string]*PartData)
	cmp.SourceName = source
	err = fileutils.WriteJSON(path+CompExt, cmp)
	return
}

// ReadCompanion deserializes a companion file based on the input path.
func ReadCompanion(path string) (cmp *Companion, err error) {
	path += CompExt
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return
	}
	cmp = &Companion{}
	err = fileutils.LoadJSON(path, cmp)
	return
}

// AddPart adds a given file "part" to the Companion.
func (cmp *Companion) AddPart(fileHash string, partHash string, prevFile string, start int64, end int64) error {
	gLock.Lock()
	defer gLock.Unlock()
	key := fmt.Sprintf("%d:%d", start, end)
	cmp.Hash = fileHash
	cmp.PrevFile = prevFile
	cmp.Parts[key] = &PartData{partHash, start, end}
	return fileutils.WriteJSON(cmp.Path+CompExt, cmp)
}

// Delete removes a given file "part" from the Companion.
func (cmp *Companion) Delete() error {
	return os.Remove(cmp.Path + CompExt)
}

// IsComplete is for determining if the corresponding file has been completely received.
func (cmp *Companion) IsComplete() bool {
	size := int64(0)
	for _, part := range cmp.Parts {
		size += part.End - part.Start
	}
	return size == cmp.Size
}
