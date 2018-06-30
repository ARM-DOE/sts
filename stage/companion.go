package stage

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/log"
)

func newLocalCompanion(path string, file *sts.Partial) (cmp *sts.Partial, err error) {
	if cmp, err = readLocalCompanion(path, file.Name); err != nil || cmp != nil {
		return
	}
	cmp = &sts.Partial{
		Name:   file.Name,
		Prev:   file.Prev,
		Size:   file.Size,
		Hash:   file.Hash,
		Source: file.Source,
	}
	return
}

func readLocalCompanion(path, name string) (cmp *sts.Partial, err error) {
	ext := filepath.Ext(path)
	if ext != compExt {
		path += compExt
	}
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	var r io.ReadCloser
	if r, err = os.Open(path); err != nil {
		return
	}
	defer r.Close()
	if cmp, err = readCompanion(r); err != nil {
		return
	}
	if cmp != nil && name != "" {
		// Because the old-style companion stored the full path and not
		// the name part
		cmp.Name = name
	}
	return
}

func readCompanion(r io.Reader) (cmp *sts.Partial, err error) {
	var b []byte
	if b, err = ioutil.ReadAll(r); err != nil {
		return
	}
	cmp = &sts.Partial{}
	if err = json.Unmarshal(b, cmp); err != nil {
		oldCmp := &oldCompanion{}
		if err = json.Unmarshal(b, oldCmp); err != nil {
			return
		}
		cmp = upgradeCompanion(oldCmp)
	}
	return
}

// ReadCompanions implements sts.DecodePartials
func ReadCompanions(r io.Reader) (cmps []*sts.Partial, err error) {
	var b []byte
	if b, err = ioutil.ReadAll(r); err != nil {
		return
	}
	cmps = []*sts.Partial{}
	if err = json.Unmarshal(b, &cmps); err != nil {
		oldCmps := []*oldCompanion{}
		if err = json.Unmarshal(b, &oldCmps); err != nil {
			return
		}
		cmps = make([]*sts.Partial, len(oldCmps))
		for i, old := range oldCmps {
			cmps[i] = upgradeCompanion(old)
		}
	}
	return
}

func upgradeCompanion(old *oldCompanion) (cmp *sts.Partial) {
	cmp = &sts.Partial{
		Hash:   old.Hash,
		Name:   old.Path,
		Prev:   old.Prev,
		Size:   old.Size,
		Source: old.Source,
	}
	for _, p := range old.Parts {
		cmp.Parts = append(cmp.Parts, &sts.ByteRange{
			Beg: p.Beg, End: p.End,
		})
	}
	return
}

func writeCompanion(path string, cmp *sts.Partial) error {
	ext := filepath.Ext(path)
	if ext != compExt {
		path += compExt
	}
	return fileutil.WriteJSON(path, cmp)
}

func addCompanionPart(cmp *sts.Partial, beg, end int64) {
	j := len(cmp.Parts)
	for i := 0; i < j; i++ {
		part := cmp.Parts[i]
		if beg >= part.End || end <= part.Beg {
			continue
		}
		log.Debug(fmt.Sprintf(
			"Remove Companion Conflict: %s => %d:%d (new) %d:%d (old)",
			cmp.Name, beg, end, part.Beg, part.End))
		cmp.Parts[j-1], cmp.Parts[i] = cmp.Parts[i], cmp.Parts[j-1]
		i--
		j--
	}
	cmp.Parts = cmp.Parts[:j]
	cmp.Parts = append(cmp.Parts, &sts.ByteRange{Beg: beg, End: end})
}

func isCompanionComplete(cmp *sts.Partial) bool {
	size := int64(0)
	for _, part := range cmp.Parts {
		size += part.End - part.Beg
	}
	return size == cmp.Size
}

type oldCompanion struct {
	Path   string              `json:"path"`
	Prev   string              `json:"prev"`
	Size   int64               `json:"size"`
	Hash   string              `json:"hash"`
	Source string              `json:"src"`
	Parts  map[string]*oldPart `json:"parts"`
}

type oldPart struct {
	Hash string `json:"hash"`
	Beg  int64  `json:"b"`
	End  int64  `json:"e"`
}

func toLegacyCompanions(partials []*sts.Partial) []*oldCompanion {
	comps := make([]*oldCompanion, len(partials))
	for i, partial := range partials {
		comps[i] = toLegacyCompanion(partial)
	}
	return comps
}

func toLegacyCompanion(partial *sts.Partial) *oldCompanion {
	cmp := &oldCompanion{
		Path:   partial.Name,
		Prev:   partial.Prev,
		Size:   partial.Size,
		Hash:   partial.Hash,
		Source: partial.Source,
		Parts:  make(map[string]*oldPart),
	}
	for _, part := range partial.Parts {
		cmp.Parts[fmt.Sprintf("%d:%d", part.Beg, part.End)] = &oldPart{
			Beg: part.Beg,
			End: part.End,
		}
	}
	return cmp
}
