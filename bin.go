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

	"github.com/ARM-DOE/sts/fileutils"
	"github.com/ARM-DOE/sts/logging"
)

// BinFluff is the percentage of the bin size that can fluctuate based on input files.
const BinFluff = 0.1

// Part is the struct for managing the parts of an outgoing "bin".
type Part struct {
	File  SendFile // The file for which this part applies
	Start int64    // Beginning byte in the part
	End   int64    // Ending byte in the part
	Hash  string   // MD5 of the data in the part file from Start:End
}

// NewPart creates a new Part reference.
func NewPart(file SendFile, start int64, end int64) *Part {
	p := &Part{}
	p.File = file
	p.Start = start
	p.End = end
	return p
}

// GetHash uses StreamMD5 to digest the file part and return its MD5.
func (part *Part) GetHash() (string, error) {
	if part.Hash == "" {
		var err error
		part.Hash, err = fileutils.PartialMD5(part.File.GetPath(), part.Start, part.End)
		if err != nil {
			return "", err
		}
	}
	return part.Hash, nil
}

// Bin is the struct for managing the chunk of data sent in a single request.
type Bin struct {
	Parts     []*Part // All the instances of Part that the Bin contains
	Bytes     int64   // Maximum amount of bytes that the Bin is able to store
	BytesLeft int64   // Unallocated bytes in the Bin
	Path      string  // Path to cached bin on disk
}

// NewBin creates a new Bin reference.
func NewBin(size int64) *Bin {
	bin := &Bin{}
	bin.Bytes = size
	bin.BytesLeft = size
	bin.Parts = make([]*Part, 0)
	return bin
}

// IsFull is for determining whether or not a bin has reached its acceptable capacity.
func (bin *Bin) IsFull() bool {
	return bin.BytesLeft < int64(BinFluff*float64(bin.Bytes)) // At least 90% full is close enough.
}

// Add adds what it can of the input SendFile to the bin.  Returns false if no bytes were added.
func (bin *Bin) Add(file SendFile) bool {
	start := file.GetBytesAlloc()
	end := int64(math.Min(float64(file.GetSize()), float64(start+bin.BytesLeft)+float64(bin.Bytes)*BinFluff))
	bytes := int64(end - start)
	if bytes > 0 {
		part := NewPart(file, start, end)
		bin.Parts = append(bin.Parts, part)
		bin.BytesLeft -= bytes
		file.AddAlloc(bytes)
		return true
	}
	return false
}

// BinEncoder is the interface for encoing a bin.
type BinEncoder interface {
	Read([]byte) (int, error)
	EncodeMeta() (string, error)
}

// BinDecoder is the interface for decoding a bin.
type BinDecoder interface {
	Next() (*PartReader, error)
}

// PartMeta is the struct that contains the part metadata needed on receiving end.
type PartMeta struct {
	Path     string
	PrevPath string
	FileHash string
	FileSize int64
	Start    int64
	End      int64
	Hash     string
}

// ZBinWriter is a wrapper around BinWriter that gzips the bin content.
type ZBinWriter struct {
	meta   []byte
	bytes  int
	bw     *BinWriter
	buff   *bytes.Buffer
	writer *gzip.Writer
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
		n, err = z.bw.Read(buff)
		logging.Debug("BIN Raw Bytes:", n, err)
		if n > 0 {
			z.writer.Write(buff[:n])
		}
		if err == io.EOF {
			z.writer.Close() // Close is important so we get the last few bytes.
			z.writer = nil   // Need to create a new one for each file.
		}
		bytes := z.buff.Bytes()
		n = len(bytes)
		copy(buff[:n], bytes)
	}
	z.bytes += n
	logging.Debug("BIN Zip Bytes:", n)
	return
}

// EncodeMeta is a wrapper around BinWriter's EncodeMeta.
func (z *ZBinWriter) EncodeMeta() (meta string, err error) {
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
	binPart      *Part    // The instance of the Part currently being operated on from Bin.Parts
	partIndex    int      // The index of the next Part from bin.Parts
	partProgress int64    // A sum of the byte counts read from the current file.
	fh           *os.File // The file handle of the currently open File that corresponds to the Bin Part
	eof          bool     // Set to true when an EOF is returned so that further calls to read do not cause an error.
}

// NewBinWriter returns a new BinWriter instance.
func NewBinWriter(bin *Bin) *BinWriter {
	if len(bin.Parts) < 1 {
		return nil
	}
	bw := &BinWriter{}
	bw.eof = false
	bw.bin = bin
	bw.partIndex = 0
	bw.startNextPart()
	return bw
}

func (bw *BinWriter) startNextPart() error {
	if bw.fh != nil {
		bw.fh.Close()
	}
	if bw.partIndex == len(bw.bin.Parts) { // If the file index will cause an error next time it is used for slicing, the Bin is finished processing.
		bw.eof = true
		return nil
	}
	var err error
	bw.partProgress = 0
	bw.binPart = bw.bin.Parts[bw.partIndex]
	bw.partIndex++
	bw.fh, err = os.Open(bw.binPart.File.GetPath())
	if err != nil {
		return fmt.Errorf("Could not open file %s while writing bin: %s", bw.binPart.File.GetPath(), err.Error())
	}
	logging.Debug("BIN Next Part:", bw.binPart.File.GetRelPath())
	bw.fh.Seek(bw.binPart.Start, 0)
	return nil
}

// Read implements io.Reader and is responsible for reading "parts" of files to the input []byte.
func (bw *BinWriter) Read(out []byte) (n int, err error) {
	if bw.eof {
		logging.Debug("BIN Done, #Parts:", len(bw.bin.Parts))
		return 0, io.EOF
	}
	// Calculate bytes left
	bytesLeft := (bw.binPart.End - bw.binPart.Start) - bw.partProgress
	// Read from file
	n, err = bw.fh.Read(out)
	bytesLeft -= int64(n)
	bw.partProgress += int64(n)
	logging.Debug("BIN Bytes Read", n, bw.binPart.File.GetRelPath())
	if err == io.EOF || n == 0 {
		bw.startNextPart()
		err = nil
	}
	return
}

// EncodeMeta is responsible for serializing the bin metadata to JSON format.
func (bw *BinWriter) EncodeMeta() (string, error) {
	pm := make([]*PartMeta, len(bw.bin.Parts))
	for i := 0; i < len(bw.bin.Parts); i++ {
		part := bw.bin.Parts[i]
		pm[i] = &PartMeta{
			Path:     part.File.GetRelPath(),
			PrevPath: part.File.GetPrevName(),
			FileHash: part.File.GetHash(),
			FileSize: part.File.GetSize(),
			Start:    part.Start,
			End:      part.End,
			Hash:     part.Hash,
		}
	}
	jsonBytes, err := json.Marshal(pm)
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
	total := pr.Meta.End - pr.Meta.Start
	left := int(total) - pr.pos
	if left > len(out) {
		left = len(out)
	}
	if pr.gzip != nil {
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
	if err != nil {
		return
	}
	return
}

// Next gets the next PartReader for this BinReader.
func (br *BinReader) Next() (pr *PartReader, eof bool) {
	if br.pi == len(br.meta) {
		eof = true
		return
	}
	pr = &PartReader{br.meta[br.pi], br.gzip, br.raw, 0}
	br.prev = pr
	br.pi++
	return
}
