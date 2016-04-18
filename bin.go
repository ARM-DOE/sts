package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"time"

	"github.com/ARM-DOE/sts/logging"
)

// BinFluff is the percentage of the bin size that can fluctuate based on input files.
const BinFluff = 0.1

// Part is the struct for managing the parts of an outgoing "bin".
type Part struct {
	File SendFile // The file for which this part applies
	Beg  int64    // Beginning byte in the part
	End  int64    // Ending byte in the part
	Hash string   // MD5 of the data in the part file from Beg:End
}

// NewPart creates a new Part reference including the hash if necessary.
func NewPart(file SendFile, beg, end int64) (p *Part, err error) {
	p = &Part{}
	p.File = file
	p.Beg = beg
	p.End = end
	// Maybe we don't need this intermediate hash validation.
	// Easy to add back in or make an option maybe...
	// if beg == 0 && end == file.GetSize() && file.GetHash() != "" {
	// 	p.Hash = file.GetHash()
	// } else {
	// 	p.Hash, err = fileutils.PartialMD5(file.GetPath(true), beg, end)
	// }
	return
}

const binCreated = 0
const binStarted = 1
const binReadStarted = 2
const binReadDone = 3
const binDone = 4

// Bin is the struct for managing the chunk of data sent in a single request.
type Bin struct {
	Parts     []*Part // All the instances of Part that the Bin contains
	Bytes     int64   // Maximum amount of bytes that the Bin is able to store
	BytesLeft int64   // Unallocated bytes in the Bin
	times     map[int]time.Time
}

// NewBin creates a new Bin reference.
func NewBin(size int64) *Bin {
	bin := &Bin{}
	bin.Bytes = size
	bin.BytesLeft = size
	bin.Parts = make([]*Part, 0)
	bin.times = make(map[int]time.Time)
	bin.setTime(binCreated)
	return bin
}

func (bin *Bin) setTime(id int) {
	bin.times[id] = time.Now()
}

// GetStarted returns the time this bin started to be read.
func (bin *Bin) GetStarted() time.Time {
	return bin.times[binStarted]
}

// SetStarted marks the time this bin is "started".
func (bin *Bin) SetStarted() {
	bin.setTime(binStarted)
}

// GetCompleted returns the time this bin was marked "done".
func (bin *Bin) GetCompleted() time.Time {
	return bin.times[binDone]
}

// SetCompleted marks the time this bin is "done".
func (bin *Bin) SetCompleted() {
	bin.setTime(binDone)
}

// IsFull is for determining whether or not a bin has reached its acceptable capacity.
func (bin *Bin) IsFull() bool {
	return bin.BytesLeft < int64(BinFluff*float64(bin.Bytes)) // At least 90% full is close enough.
}

// Add adds what it can of the input SendFile to the bin.  Returns false if no bytes were added.
func (bin *Bin) Add(file SendFile) (added bool, err error) {
	beg, end := file.GetNextAlloc()
	end = int64(math.Min(float64(end), float64(beg+bin.BytesLeft)+float64(bin.Bytes)*BinFluff))
	bytes := int64(end - beg)
	if bytes > 0 {
		var p *Part
		p, err = NewPart(file, beg, end)
		if err != nil {
			return
		}
		bin.Parts = append(bin.Parts, p)
		bin.BytesLeft -= bytes
		logging.Debug("BIN Allocating:", file.GetRelPath(), beg, end)
		file.AddAlloc(bytes)
		added = true
		return
	}
	return
}

// Validate loops over the bin parts and extracts (removes) any changed or otherwise problem files
// and returns them (after setting them to canceled).
func (bin *Bin) Validate() []SendFile {
	var pass []*Part
	var fail []SendFile
	b := int64(0)
	for _, p := range bin.Parts {
		if p.File.GetCancel() { // If another part of this file was already canceled.
			fail = append(fail, p.File)
			continue
		}
		changed, err := p.File.Stat()
		if changed || err != nil {
			p.File.SetCancel(true)
			fail = append(fail, p.File)
			continue
		}
		pass = append(pass, p)
		b += p.End - p.Beg
	}
	bin.Parts = pass
	bin.Bytes = b
	return fail
}

// Split splits a bin after n parts.
// The new bin is from position "n" forward.
func (bin *Bin) Split(n int) (b *Bin) {
	if n < 1 || n >= len(bin.Parts) {
		return nil
	}
	nb := int64(0)
	for i := n; i < len(bin.Parts); i++ {
		nb += bin.Parts[i].End - bin.Parts[i].Beg
	}
	b = NewBin(nb)
	b.BytesLeft = 0
	b.Parts = bin.Parts[n:]
	bin.Parts = bin.Parts[:n]
	bin.Bytes -= nb
	bin.BytesLeft = 0
	return b
}

// PartMeta is the struct that contains the part metadata needed on receiving end.
// Keeping the JSON encoding as lite as possible to minimize payload.
type PartMeta struct {
	Path     string `json:"n"`
	PrevPath string `json:"p"`
	FileHash string `json:"f"`
	FileSize int64  `json:"s"`
	Beg      int64  `json:"b"`
	End      int64  `json:"e"`
	Hash     string `json:"h"`
}

// BinEncoder is the struct that manages writing a bin.
type BinEncoder struct {
	bin          *Bin
	binPart      *Part       // The instance of the Part currently being operated on from Bin.Parts
	partIndex    int         // The index of the next Part from bin.Parts
	partProgress int64       // A sum of the byte counts read from the current file.
	fh           *os.File    // The file handle of the currently open File that corresponds to the Bin Part
	eop          bool        // Set to true when a part is completely read.
	eob          bool        // Set to true when when the bin is completely read.
	Meta         []*PartMeta // The encoded metadata for this bin to be included in the payload.
}

// NewBinEncoder returns a new BinWriter instance.
func NewBinEncoder(bin *Bin) *BinEncoder {
	if len(bin.Parts) < 1 {
		return nil
	}
	b := &BinEncoder{}
	b.bin = bin
	b.Meta = make([]*PartMeta, len(b.bin.Parts))
	for i := 0; i < len(b.bin.Parts); i++ {
		part := b.bin.Parts[i]
		b.Meta[i] = &PartMeta{
			Path:     part.File.GetRelPath(),
			PrevPath: part.File.GetPrevName(),
			FileHash: part.File.GetHash(),
			FileSize: part.File.GetSize(),
			Beg:      part.Beg,
			End:      part.End,
			Hash:     part.Hash,
		}
	}
	return b
}

func (b *BinEncoder) startNextPart() error {
	if b.fh != nil {
		b.fh.Close()
	}
	if b.partIndex >= len(b.bin.Parts) { // If the file index will cause an error next time it is used for slicing, the Bin is finished processing.
		b.bin.setTime(binReadDone)
		b.eob = true
		return nil
	}
	if b.partIndex == 0 {
		b.bin.setTime(binReadStarted)
	}
	var err error
	b.partProgress = 0
	b.binPart = b.bin.Parts[b.partIndex]
	b.partIndex++
	b.fh, err = os.Open(b.binPart.File.GetPath(true))
	if err != nil {
		return fmt.Errorf("Could not open file %s while writing bin: %s", b.binPart.File.GetPath(true), err.Error())
	}
	logging.Debug("BIN Next Part:", b.binPart.File.GetRelPath(), b.binPart.Beg)
	b.fh.Seek(b.binPart.Beg, 0)
	return nil
}

// Read reads the next bit of bytes from the current file part.
func (b *BinEncoder) Read(p []byte) (n int, err error) {
	if b.fh == nil {
		b.startNextPart()
	}
	b.eop = false
	// Calculate bytes left
	bytesLeft := (b.binPart.End - b.binPart.Beg) - b.partProgress
	n = len(p)
	if bytesLeft < int64(n) {
		// If this part of the file is smaller than the buffer and we're not to the
		// end of the file yet, we don't want to lose bytes.
		n = int(bytesLeft)
	}
	// Read from file
	n, err = b.fh.Read(p[:n])
	bytesLeft -= int64(n)
	b.partProgress += int64(n)
	logging.Debug("BIN Bytes Read", n, b.binPart.File.GetRelPath(), bytesLeft)
	if err == io.EOF || n == 0 || bytesLeft == 0 {
		err = b.startNextPart()
		b.eop = true
		if err == nil && b.eob {
			err = io.EOF
		}
	}
	return
}

// PartDecoder is responsible for parsing individual "parts" of "bin" requests.
type PartDecoder struct {
	Meta *PartMeta
	Hash hash.Hash
	r    io.Reader
	pos  int
}

// Read reads the incoming stream (either raw or gzipped) to the input []byte.
func (pr *PartDecoder) Read(out []byte) (n int, err error) {
	total := pr.Meta.End - pr.Meta.Beg
	left := int(total) - pr.pos
	if left > len(out) {
		left = len(out)
	}
	n, err = pr.r.Read(out[:left])
	// Only compute the hash if we were given one to compare against.
	if pr.Meta.Hash != "" {
		pr.Hash.Write(out[:n])
	}
	pr.pos += n
	if pr.pos == int(total) {
		logging.Debug("BIN Part Done", pr.pos)
		return n, io.EOF
	}
	if pr.pos > int(total) {
		logging.Error("BIN Part Overflow:", pr.pos-int(total))
	}
	logging.Debug("BIN Part Read", n, pr.pos, total, err)
	return
}

// BinDecoder is responsible for parsing "bin" requests on the receiving end.
type BinDecoder struct {
	r    io.Reader
	meta []*PartMeta
	prev *PartDecoder
	pi   int
}

// NewBinDecoder returns a new instance of BinWriter.
func NewBinDecoder(r io.Reader, n int) (br *BinDecoder, err error) {
	br = &BinDecoder{}
	br.r = r
	br.pi = 0
	pr, pw := io.Pipe()
	go func() {
		io.CopyN(pw, r, int64(n))
	}()
	jr := json.NewDecoder(pr)
	err = jr.Decode(&br.meta)
	return
}

// Next gets the next PartDecoder for this BinDecoder.
func (b *BinDecoder) Next() (p *PartDecoder, eof bool) {
	if b.pi == len(b.meta) {
		eof = true
		return
	}
	p = &PartDecoder{
		Meta: b.meta[b.pi],
		Hash: md5.New(),
		r:    b.r,
	}
	b.prev = p
	b.pi++
	return
}
