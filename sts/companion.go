package sts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"code.arm.gov/dataflow/sts/fileutils"
	"code.arm.gov/dataflow/sts/logging"
)

// CompExt is the extension added to "companion" files on the receiving end.
const CompExt = ".cmp"

// Companion provides the functionality for working with "companion" files.
type Companion struct {
	Path       string               `json:"path"`
	PrevFile   string               `json:"prev"`
	Size       int64                `json:"size"`
	SourceName string               `json:"src"`
	Parts      map[string]*PartData `json:"parts"`
	Hash       string               `json:"hash"`
}

// PartData holds the information for a particular "part" of a file.  A companion has 1..N parts.
type PartData struct {
	Hash string `json:"hash"`
	Beg  int64  `json:"b"`
	End  int64  `json:"e"`
}

// NewCompanion returns a new Companion reference, which includes reading the JSON-cached counterpart, if it exists.
func NewCompanion(source string, path string, prevFile string, size int64, hash string) (cmp *Companion, err error) {
	cmp, err = ReadCompanion(path)
	if cmp != nil && err == nil {
		return
	}
	cmp = &Companion{}
	cmp.Path = path
	cmp.PrevFile = prevFile
	cmp.Size = size
	cmp.Hash = hash
	cmp.Parts = make(map[string]*PartData)
	cmp.SourceName = source
	err = cmp.Write()
	return
}

// ReadCompanion deserializes a companion file based on the input path.
func ReadCompanion(path string) (cmp *Companion, err error) {
	ext := filepath.Ext(path)
	p := path
	if ext != CompExt {
		path += CompExt
	} else {
		p = strings.TrimSuffix(path, ext)
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return
	}
	cmp = &Companion{}
	err = fileutils.LoadJSON(path, cmp)
	if err != nil {
		return
	}
	if cmp.Path != p {
		cmp.Path = p // Just to make sure.
	}
	return
}

// AddPart adds a given file "part" to the Companion.
func (cmp *Companion) AddPart(partHash string, beg int64, end int64) {
	for key, part := range cmp.Parts {
		if end <= part.Beg || beg >= part.End {
			continue
		}
		logging.Debug(fmt.Sprintf("COMPANION Remove Conflict: %s => %d:%d (new) %d:%d (old)", cmp.Path, beg, end, part.Beg, part.End))
		delete(cmp.Parts, key)
	}
	key := fmt.Sprintf("%d:%d", beg, end)
	cmp.Parts[key] = &PartData{partHash, beg, end}
}

func (cmp *Companion) Write() error {
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
		size += part.End - part.Beg
	}
	return size == cmp.Size
}
