package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ARM-DOE/sts/fileutils"
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
	if beg == 0 && end == file.GetSize() && file.GetHash() != "" {
		p.Hash = file.GetHash()
	} else {
		p.Hash, err = fileutils.PartialMD5(file.GetPath(true), beg, end)
	}
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

// BinEncoder is the interface for encoing a bin.
type BinEncoder interface {
	Read([]byte) (int, error)
	EncodeMeta() (string, error)
}

// PartMeta is the struct that contains the part metadata needed on receiving end.
// Keeping the JSON encoding as lite as possible to minimize payload.
type PartMeta struct {
	Path       string `json:"n"`
	PrevPath   string `json:"p"`
	FileHash   string `json:"f"`
	FileSize   int64  `json:"s"`
	Compressed bool   `json:"z"`
	Beg        int64  `json:"b"`
	End        int64  `json:"e"`
	Hash       string `json:"h"`
}

// ZBinWriter is a wrapper around BinWriter that gzips the bin content.
type ZBinWriter struct {
	meta   []byte
	bytes  int
	bw     *BinWriter
	buff   *bytes.Buffer
	writer *gzip.Writer
	skip   bool
	n      int
}

// NewZBinWriter returns a new ZBinWriter instance.
func NewZBinWriter(bw *BinWriter) *ZBinWriter {
	z := &ZBinWriter{}
	z.bw = bw
	z.buff = &bytes.Buffer{}
	return z
}

// Read implements io.Reader and wraps around BinWriter.  Its job is to gzip what it reads.
func (z *ZBinWriter) Read(buff []byte) (n int, err error) {
	z.buff.Reset()
	if z.writer == nil {
		z.writer, err = gzip.NewWriterLevel(z.buff, gzip.BestCompression)
		if err != nil {
			return
		}
	}
	if z.bytes < len(z.meta) {
		n = int(math.Min(float64(len(buff)), float64(len(z.meta)-z.bytes)))
		copy(buff, z.meta[z.bytes:n])
	} else {
		if z.bw.partProgress == 0 {
			i := z.bw.partIndex
			if i > 0 {
				i-- // Usually the partIndex is one ahead but not true at the outset.
			}
			z.skip = !z.bw.meta[i].Compressed
		}
		if z.skip {
			// If we think the file is already compressed, let's not waste the CPU cycles.
			n, err = z.bw.Read(buff)
			return
		}
		// We don't want to read the full length because of the possibility that the gzipped
		// content will actually be slightly larger than the original.  Rare, but happens.
		if z.n == 0 {
			z.n = int(math.Ceil(float64(len(buff)) * float64(0.9)))
		}
		n, err = z.bw.Read(buff[:z.n])
		logging.Debug("BIN Raw Bytes:", n, err)
		if n > 0 {
			z.writer.Write(buff[:n])
			z.writer.Flush() // Flush is important so we don't overrun the buffer later (on Close)
		}
		if z.bw.eop {
			if z.bw.eob {
				logging.Debug("BIN End-of-Bin")
			} else {
				logging.Debug("BIN End-of-Part")
			}
			z.writer.Close() // Close is important so we get the last few bytes.
			z.writer.Reset(z.buff)
		}
		bytes := z.buff.Bytes()
		n = len(bytes)
		if n > len(buff) {
			// TODO: figure out a way to recover from this.
			err = fmt.Errorf("Bin gzip buffer got overrun by %d bytes", n-len(buff))
			n = 0
			return
		}
		copy(buff, bytes)
	}
	z.bytes += n
	logging.Debug("BIN Zip Bytes:", n)
	return
}

// EncodeMeta is a wrapper around BinWriter's EncodeMeta.
func (z *ZBinWriter) EncodeMeta() (meta string, err error) {
	for i, p := range z.bw.meta {
		p.Compressed = !fileutils.GuessCompressed(z.bw.bin.Parts[i].File.GetPath(true))
	}
	z.buff.Reset()
	z.writer, err = gzip.NewWriterLevel(z.buff, gzip.BestCompression)
	if err != nil {
		return
	}
	meta, err = z.bw.EncodeMeta()
	if err != nil {
		return
	}
	bmeta := []byte(meta)
	z.writer.Write(bmeta)
	z.writer.Flush()
	z.writer.Close()
	z.writer = nil
	z.meta = z.buff.Bytes()
	meta = fmt.Sprintf("%d", len(bmeta))
	return
}

// BinWriter is the struct that manages writing a bin.
type BinWriter struct {
	bin          *Bin
	binPart      *Part       // The instance of the Part currently being operated on from Bin.Parts
	partIndex    int         // The index of the next Part from bin.Parts
	partProgress int64       // A sum of the byte counts read from the current file.
	fh           *os.File    // The file handle of the currently open File that corresponds to the Bin Part
	eop          bool        // Set to true when a part is completely read.
	eob          bool        // Set to true when when the bin is completely read.
	meta         []*PartMeta // The encoded metadata for this bin to be included in the payload.
}

// NewBinWriter returns a new BinWriter instance.
func NewBinWriter(bin *Bin) *BinWriter {
	if len(bin.Parts) < 1 {
		return nil
	}
	bw := &BinWriter{}
	bw.bin = bin
	bw.meta = make([]*PartMeta, len(bw.bin.Parts))
	for i := 0; i < len(bw.bin.Parts); i++ {
		part := bw.bin.Parts[i]
		bw.meta[i] = &PartMeta{
			Path:     part.File.GetRelPath(),
			PrevPath: part.File.GetPrevName(),
			FileHash: part.File.GetHash(),
			FileSize: part.File.GetSize(),
			Beg:      part.Beg,
			End:      part.End,
			Hash:     part.Hash,
		}
	}
	return bw
}

func (bw *BinWriter) startNextPart() error {
	if bw.fh != nil {
		bw.fh.Close()
	}
	if bw.partIndex >= len(bw.bin.Parts) { // If the file index will cause an error next time it is used for slicing, the Bin is finished processing.
		bw.bin.setTime(binReadDone)
		bw.eob = true
		return nil
	}
	if bw.partIndex == 0 {
		bw.bin.setTime(binReadStarted)
	}
	var err error
	bw.partProgress = 0
	bw.binPart = bw.bin.Parts[bw.partIndex]
	bw.partIndex++
	bw.fh, err = os.Open(bw.binPart.File.GetPath(true))
	if err != nil {
		return fmt.Errorf("Could not open file %s while writing bin: %s", bw.binPart.File.GetPath(true), err.Error())
	}
	logging.Debug("BIN Next Part:", bw.binPart.File.GetRelPath(), bw.binPart.Beg)
	bw.fh.Seek(bw.binPart.Beg, 0)
	return nil
}

// Read implements io.Reader and is responsible for reading "parts" of files to the input []byte.
func (bw *BinWriter) Read(out []byte) (n int, err error) {
	if bw.fh == nil {
		bw.startNextPart()
	}
	bw.eop = false
	// Calculate bytes left
	bytesLeft := (bw.binPart.End - bw.binPart.Beg) - bw.partProgress
	n = len(out)
	if bytesLeft < int64(n) {
		// If this part of the file is smaller than the buffer and we're not to the
		// end of the file yet, we don't want to lose bytes.
		n = int(bytesLeft)
	}
	// Read from file
	n, err = bw.fh.Read(out[:n])
	bytesLeft -= int64(n)
	bw.partProgress += int64(n)
	logging.Debug("BIN Bytes Read", n, bw.binPart.File.GetRelPath(), bytesLeft)
	if err == io.EOF || n == 0 || bytesLeft == 0 {
		err = bw.startNextPart()
		bw.eop = true
		if err == nil && bw.eob {
			err = io.EOF
		}
	}
	return
}

// EncodeMeta is responsible for serializing the bin metadata to JSON format.
func (bw *BinWriter) EncodeMeta() (string, error) {
	jsonBytes, err := json.Marshal(bw.meta)
	if err != nil {
		return "", err
	}
	return string(jsonBytes[0:len(jsonBytes)]), nil
}

// PartReader is responsible for parsing individual "parts" of "bin" requests.
type PartReader struct {
	Meta *PartMeta
	gzip *gzip.Reader
	raw  io.Reader
	pos  int
}

// Read reads the incoming stream (either raw or gzipped) and writes it to the input []byte.
func (pr *PartReader) Read(out []byte) (n int, err error) {
	total := pr.Meta.End - pr.Meta.Beg
	left := int(total) - pr.pos
	if left > len(out) {
		left = len(out)
	}
	if pr.gzip != nil && pr.Meta.Compressed {
		n, err = pr.gzip.Read(out[:left])
	} else {
		n, err = pr.raw.Read(out[:left])
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

// BinReader is responsible for parsing "bin" requests on the receiving end.
type BinReader struct {
	raw  io.Reader
	gzip *gzip.Reader
	meta []*PartMeta
	prev *PartReader
	pi   int
}

// NewBinReader returns a new instance of BinReader.
func NewBinReader(encMeta string, body io.Reader, compressed bool) (br *BinReader, err error) {
	br = &BinReader{}
	br.raw = body
	br.pi = 0
	if compressed {
		br.gzip, err = gzip.NewReader(body)
		if err != nil {
			return
		}
		var n int
		n, err = strconv.Atoi(encMeta) // The length of the unzipped metadata.
		buff := make([]byte, n)
		br.gzip.Read(buff)
		encMeta = string(buff[:n])
	}
	fromJSON := json.NewDecoder(strings.NewReader(encMeta))
	err = fromJSON.Decode(&br.meta)
	return
}

// Next gets the next PartReader for this BinReader.
func (br *BinReader) Next() (pr *PartReader, eof bool) {
	if br.pi == len(br.meta) {
		eof = true
		return
	}
	pr = &PartReader{
		Meta: br.meta[br.pi],
		gzip: br.gzip,
		raw:  br.raw,
	}
	br.prev = pr
	br.pi++
	return
}
