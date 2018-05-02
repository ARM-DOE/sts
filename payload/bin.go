package payload

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
)

// BinFluff is the percentage of the bin size that can fluctuate based on
// input files
const binFluff = 0.1

type fileMeta struct {
	Name string `json:"n"`
	Prev string `json:"p"`
	Hash string `json:"f"`
	Time int64  `json:"t"`
	Size int64  `json:"s"`
	Beg  int64  `json:"b"`
	End  int64  `json:"e"`
	send int64
}

func (f *fileMeta) GetName() string {
	return f.Name
}

func (f *fileMeta) GetPrev() string {
	return f.Prev
}

func (f *fileMeta) GetFileHash() string {
	return f.Hash
}

func (f *fileMeta) GetFileSize() int64 {
	return f.Size
}

func (f *fileMeta) GetFileTime() int64 {
	return f.Time
}

func (f *fileMeta) GetSlice() (int64, int64) {
	return f.Beg, f.End
}

func (f *fileMeta) GetSendSize() int64 {
	return f.send
}

type part struct {
	sts.Binnable       // The file for which this part applies
	beg          int64 // Beginning byte in the part
	end          int64 // Ending byte in the part
}

func (p *part) GetFileSize() int64 {
	return p.Binnable.GetSize()
}

func (p *part) GetFileTime() int64 {
	return p.Binnable.GetTime()
}

func (p *part) GetFileHash() string {
	return p.Binnable.GetHash()
}

func (p *part) GetSlice() (int64, int64) {
	return p.beg, p.end - p.beg
}

const (
	binCreated     = 0
	binStarted     = 1
	binReadStarted = 2
	binReadDone    = 3
	binDone        = 4
)

// Bin is the struct for managing the chunk of data sent in a single request
type Bin struct {
	parts    []*part // All the instances of Part that the Bin contains
	capacity int64   // Optimal amount of bytes that the Bin is able to store
	fluff    int64   // Allowed fluff
	bytes    int64   // Allocated bytes in the Bin
	times    map[int]time.Time
	opener   sts.Open
}

// NewBin creates a new Bin reference
func NewBin(size int64, opener sts.Open) sts.Payload {
	bin := &Bin{
		capacity: size,
		fluff:    int64(float64(size) * binFluff),
		parts:    make([]*part, 0),
		times:    make(map[int]time.Time),
		opener:   opener,
	}
	bin.setTime(binCreated)
	return bin
}

func (bin *Bin) setTime(id int) {
	bin.times[id] = time.Now()
}

// GetStarted returns the time this bin started to be read
func (bin *Bin) GetStarted() time.Time {
	return bin.times[binStarted]
}

// SetStarted marks the time this bin is "started"
func (bin *Bin) setStarted() {
	bin.setTime(binStarted)
}

// GetCompleted returns the time this bin was marked "done"
func (bin *Bin) GetCompleted() time.Time {
	return bin.times[binDone]
}

// SetCompleted marks the time this bin is "done"
func (bin *Bin) setCompleted() {
	bin.setTime(binDone)
}

// IsFull is for determining whether or not a bin has reached its acceptable
// capacity
func (bin *Bin) IsFull() bool {
	space := bin.capacity - bin.bytes
	return space < 0 || space < bin.fluff
}

// Add adds what it can of the input Binnable to the bin.  Returns false if no
// bytes were added.
func (bin *Bin) Add(chunk sts.Binnable) (added bool) {
	beg, end := chunk.GetNextAlloc()
	space := (bin.capacity + bin.fluff) - bin.bytes
	end = int64(math.Min(
		float64(end),
		float64(beg+space)))
	bytes := end - beg
	if bytes > 0 {
		p := &part{
			Binnable: chunk,
			beg:      beg,
			end:      end,
		}
		bin.parts = append(bin.parts, p)
		bin.bytes += bytes
		chunk.AddAlloc(bytes)
		added = true
		return
	}
	log.Debug("Not binned:", chunk.GetName(), beg, end, bin.capacity, bin.bytes, bin.fluff)
	return
}

// Remove removes the part specified
func (bin *Bin) Remove(binned sts.Binned) {
	index := -1
	for i, p := range bin.parts {
		if p == binned {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	n := len(bin.parts)
	bin.parts[n-1], bin.parts[index] = bin.parts[index], bin.parts[n-1]
	bin.parts = bin.parts[:n-1]
	bin.bytes -= (binned.(*part).end - binned.(*part).beg)
}

// Split splits a bin after n parts; the new bin is from position "n" forward
func (bin *Bin) Split(n int) sts.Payload {
	if n < 1 || n >= len(bin.parts) {
		return nil
	}
	nb := int64(0)
	for i := n; i < len(bin.parts); i++ {
		nb += bin.parts[i].end - bin.parts[i].beg
	}
	b := NewBin(nb, bin.opener).(*Bin)
	b.capacity = nb
	b.bytes = nb
	b.parts = bin.parts[n:]
	bin.parts = bin.parts[:n]
	bin.capacity = bin.bytes - nb
	bin.bytes = bin.capacity
	bin.setCompleted()
	return b
}

// GetParts returns the slice of inner binnables that make up the payload
func (bin *Bin) GetParts() []sts.Binned {
	parts := make([]sts.Binned, len(bin.parts))
	for i, p := range bin.parts {
		parts[i] = p
	}
	return parts
}

// GetSize returns the total size of this bin in bytes
func (bin *Bin) GetSize() int64 {
	return bin.bytes
}

// EncodeHeader returns a byte-array encoding of the metadata for this payload
func (bin *Bin) EncodeHeader() (byteMeta []byte, err error) {
	meta := make([]*fileMeta, len(bin.parts))
	for i := 0; i < len(bin.parts); i++ {
		part := bin.parts[i]
		meta[i] = &fileMeta{
			Name: part.GetName(),
			Prev: part.GetPrev(),
			Hash: part.GetFileHash(),
			Time: part.GetFileTime(),
			Size: part.GetFileSize(),
			send: part.GetSendSize(),
			Beg:  part.beg,
			End:  part.end,
		}
	}
	byteMeta, err = json.Marshal(meta)
	return
}

// GetEncoder returns an io.Reader for reading the bin content
func (bin *Bin) GetEncoder() io.Reader {
	return NewEncoder(bin)
}

// Encoder is the struct that manages writing a bin
type Encoder struct {
	bin          *Bin
	binPart      *part // The instance of the Part currently being operated on from Bin.Parts
	partIndex    int   // The index of the next Part from bin.Parts
	partProgress int64 // A sum of the byte counts read from the current file.
	eop          bool  // Set to true when a part is completely read.
	eob          bool  // Set to true when when the bin is completely read.
	handle       sts.Readable
}

// NewEncoder returns a new BinWriter instance
func NewEncoder(bin *Bin) *Encoder {
	if len(bin.parts) < 1 {
		return nil
	}
	b := &Encoder{}
	b.bin = bin
	return b
}

func (b *Encoder) startNextPart() error {
	if b.handle != nil {
		b.handle.Close()
	}
	// If the file index will cause an error next time it is used for slicing,
	// the Bin is finished processing
	if b.partIndex >= len(b.bin.parts) {
		b.bin.setTime(binReadDone)
		b.eob = true
		return nil
	}
	if b.partIndex == 0 {
		b.bin.setTime(binReadStarted)
	}
	var err error
	b.partProgress = 0
	b.binPart = b.bin.parts[b.partIndex]
	b.partIndex++
	if b.handle, err = b.bin.opener(b.binPart.Binnable); err != nil {
		return fmt.Errorf(
			"Failed to open %s for bin writing: %s",
			b.binPart.GetPath(),
			err.Error())
	}
	if _, err = b.handle.Seek(b.binPart.beg, 0); err != nil {
		return fmt.Errorf(
			"Failed to seek %s:%d while writing bin: %s",
			b.binPart.GetPath(),
			b.binPart.beg,
			err.Error())
	}
	return nil
}

// Read reads the next bit of bytes from the current file part
func (b *Encoder) Read(p []byte) (n int, err error) {
	if b.partIndex == 0 {
		b.bin.setStarted()
	}
	if b.binPart == nil {
		b.startNextPart()
	}
	b.eop = false
	// Calculate bytes left
	bytesLeft := (b.binPart.end - b.binPart.beg) - b.partProgress
	n = len(p)
	if bytesLeft < int64(n) {
		// If this part of the file is smaller than the buffer and we're not to
		// the end yet, we don't want to lose bytes
		n = int(bytesLeft)
	}
	// Read from file.
	// If our buffer is bigger than what the reader will read, let's read until
	// our buffer is full if we can
	nn := 0
	var nread int
	for {
		nread, err = b.handle.Read(p[nn:n])
		nn += nread
		if err == nil && nn < n {
			continue
		}
		break
	}
	bytesLeft -= int64(n)
	b.partProgress += int64(n)
	// logging.Debug("BIN Bytes Read", n, b.binPart.File.GetName(), bytesLeft)
	if err == io.EOF || n == 0 || bytesLeft == 0 {
		err = b.startNextPart()
		b.eop = true
		if err == nil && b.eob {
			err = io.EOF
		}
		b.bin.setCompleted()
	}
	return
}

// PartDecoder is responsible for parsing individual "parts" of "bin" requests
type PartDecoder struct {
	meta   *fileMeta
	stream io.Reader
	pos    int
}

// Read reads the incoming stream (either raw or gzipped) to the input []byte
func (pr *PartDecoder) Read(out []byte) (n int, err error) {
	total := pr.meta.End - pr.meta.Beg
	left := int(total) - pr.pos
	if left > len(out) {
		left = len(out)
	}
	n, err = pr.stream.Read(out[:left])
	pr.pos += n
	if pr.pos == int(total) {
		return n, io.EOF
	}
	if pr.pos > int(total) {
		log.Error("BIN Part Overflow:", pr.pos-int(total))
	}
	return
}

// Decoder is responsible for parsing "bin" requests on the receiving end
type Decoder struct {
	stream    io.Reader
	meta      []*fileMeta
	partIndex int
}

// NewDecoder expects the number of bytes devoted to the metadata array and the
// path delimeter string as inputs
func NewDecoder(n int, r io.Reader) (sts.PayloadDecoder, error) {
	var err error
	binReader := &Decoder{
		stream: r,
	}
	pr, pw := io.Pipe()
	go func() {
		io.CopyN(pw, r, int64(n))
	}()
	jr := json.NewDecoder(pr)
	err = jr.Decode(&binReader.meta)
	return binReader, err
}

// GetParts returns the part metadata
func (b *Decoder) GetParts() []sts.Binned {
	parts := make([]sts.Binned, len(b.meta))
	for i, p := range b.meta {
		parts[i] = p
	}
	return parts
}

// Next gets the next part decoder for this bin decoder
func (b *Decoder) Next() (io.Reader, bool) {
	if b.partIndex == len(b.meta) {
		return nil, true
	}
	p := &PartDecoder{
		meta:   b.meta[b.partIndex],
		stream: b.stream,
	}
	b.partIndex++
	return p, false
}
