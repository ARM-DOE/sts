package send

import (
	"fmt"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/bin"
	"code.arm.gov/dataflow/sts/logging"
)

// Must implement File, Sendable, Binnable, and Sent.
type sendableChunk struct {
	file       sts.Sendable
	bytesAlloc int64
	bytesSent  int64
	started    time.Time
	completed  time.Time
	lock       sync.RWMutex
}

func newSendableChunk(file sts.Sendable) *sendableChunk {
	return &sendableChunk{file: file}
}

func (sc *sendableChunk) GetPath(follow bool) string {
	return sc.file.GetPath(follow)
}

func (sc *sendableChunk) GetRelPath() string {
	return sc.file.GetRelPath()
}

func (sc *sendableChunk) GetSize() int64 {
	return sc.file.GetSize()
}

func (sc *sendableChunk) GetTime() int64 {
	return sc.file.GetTime()
}

func (sc *sendableChunk) GetHash() string {
	return sc.file.GetHash()
}

func (sc *sendableChunk) GetPrevName() string {
	return sc.file.GetPrevName()
}

func (sc *sendableChunk) GetSlice() (int64, int64) {
	return sc.file.GetSlice()
}

func (sc *sendableChunk) GetNextAlloc() (int64, int64) {
	_, length := sc.file.GetSlice()
	return sc.bytesAlloc, length
}

func (sc *sendableChunk) AddAlloc(bytes int64) {
	sc.bytesAlloc += bytes
}

func (sc *sendableChunk) isAllocated() bool {
	_, length := sc.file.GetSlice()
	return sc.bytesAlloc == length
}

func (sc *sendableChunk) addSent(bytes int64) bool {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	sc.bytesSent += bytes
	return sc.isSent()
}

func (sc *sendableChunk) isSent() bool {
	_, length := sc.file.GetSlice()
	return sc.bytesSent == length
}

func (sc *sendableChunk) setStarted(t time.Time) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if sc.started.IsZero() || t.Before(sc.started) {
		sc.started = t
	}
}

func (sc *sendableChunk) setCompleted(t time.Time) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if t.After(sc.completed) {
		sc.completed = t
	}
}

func (sc *sendableChunk) TimeMs() int64 {
	if !sc.completed.IsZero() && !sc.started.IsZero() {
		return int64(sc.completed.Sub(sc.started).Nanoseconds() / 1e6)
	}
	return -1
}

// Sender is the struct for managing STS send operations.
type Sender struct {
	conf             *sts.SendConf
	in               <-chan sts.Sendable
	done             chan<- []sts.Sent
	sendBin          chan *bin.Bin
	doneBin          chan *bin.Bin
	sendFunc         func(*bin.Bin) (int, error)
	statSince        time.Time
	statLock         sync.RWMutex
	bytesOut         int64
	bytesPerSecCount int64
	bytesPerSecSum   float64
}

// NewSender creates a new sender instance based on provided configuration.
func NewSender(conf *sts.SendConf, sendFunc func(*bin.Bin) (int, error)) (s *Sender, err error) {
	s = &Sender{
		conf:     conf,
		sendFunc: sendFunc,
	}
	return
}

// Start controls the sending of files on the in channel and writing results to the out channels.
func (sender *Sender) Start(in <-chan sts.Sendable, done chan<- []sts.Sent) {
	sender.in = in
	sender.done = done
	sender.sendBin = make(chan *bin.Bin, sender.conf.Threads*2)
	sender.doneBin = make(chan *bin.Bin, sender.conf.Threads)
	var wgBin, wgSend, wgDone sync.WaitGroup
	nBin, nSend, nDone := 1, sender.conf.Threads, 1
	start := func(s func(wg *sync.WaitGroup), wg *sync.WaitGroup, n int) {
		wg.Add(n)
		for i := 0; i < n; i++ {
			go s(wg)
		}
	}

	// Start the go routines.
	start(sender.startBinning, &wgBin, nBin)
	start(sender.startSending, &wgSend, nSend)
	start(sender.startNotifying, &wgDone, nDone)

	wgBin.Wait()
	logging.Debug("SEND Binning Done")
	close(sender.sendBin)
	wgSend.Wait()
	logging.Debug("SEND Sending Done")
	close(sender.doneBin)
	wgDone.Wait()
	logging.Debug("SEND Notifying Done")
	close(sender.done)
}

// func (sender *Sender) startWrap(wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	for {
// 		f, ok := <-sender.ch.In
// 		if !ok {
// 			break
// 		}
// 		logging.Debug("SEND Found:", f.GetRelPath())
// 		sf, err := newSendFile(f)
// 		if err != nil {
// 			logging.Error("Failed to get file ready to send:", f.GetPath(false), err.Error())
// 			return
// 		}
// 		if sf.IsSent() {
// 			logging.Debug("SEND File Already Done:", f.GetRelPath())
// 			// If we get here then this is a "RecoverFile" that was fully sent
// 			// before a crash and we need to make sure it got logged as "sent"
// 			// and if not, we need to do it, even though we won't know how long
// 			// the original transfer took.
// 			if !logging.FindSent(sf.GetRelPath(), time.Unix(sf.GetTime(), 0), time.Now(), sender.conf.TargetName) {
// 				logging.Sent(sf.GetRelPath(), sf.GetHash(), sf.GetSize(), 0, sender.conf.TargetName)
// 			}
// 			for _, c := range sender.ch.Done {
// 				c <- []sts.SendFile{sf}
// 			}
// 			continue
// 		}
// 		sender.ch.sendFile <- sf
// 	}
// }

func (sender *Sender) startBinning(wg *sync.WaitGroup) {
	defer wg.Done()
	var sendable sts.Sendable
	var currBin *bin.Bin
	var currChunk *sendableChunk
	var waited bool
	var ok bool
	for {
		if sender.in == nil {
			logging.Debug("SEND Exit Binning")
			return
		}
		switch {
		case currChunk == nil && currBin == nil:
			if sendable, ok = <-sender.in; !ok {
				sender.in = nil
				continue
			}
			currChunk = newSendableChunk(sendable)
		case currChunk == nil:
			select {
			case sendable, ok = <-sender.in:
				if !ok {
					sender.in = nil
					sender.sendBin <- currBin
					continue
				}
				currChunk = newSendableChunk(sendable)
			default:
				if !waited {
					time.Sleep(100 * time.Millisecond)
					waited = true
					continue
				}
				sender.sendBin <- currBin
				currBin = nil
				continue
			}
		}
		if currBin == nil {
			currBin = bin.NewBin(int64(sender.conf.BinSize))
			waited = false
		}
		currBin.Add(currChunk)
		if currChunk.isAllocated() {
			currChunk = nil
		}
		if currBin.IsFull() {
			sender.sendBin <- currBin
			currBin = nil
		}
	}
}

func (sender *Sender) startSending(wg *sync.WaitGroup) {
	defer wg.Done()
	var t time.Time
	var bin *bin.Bin
	var ok bool
	for {
		t = time.Now()
		if bin, ok = <-sender.sendBin; !ok {
			break
		}
		logging.Debug("SEND Idle:", time.Since(t))
		for _, part := range bin.Parts {
			logging.Debug("SEND Bin Part:", &bin, part.File.GetRelPath(), part.Beg, part.End)
		}
		sender.send(bin)
	}
}

func (sender *Sender) startNotifying(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		bin, ok := <-sender.doneBin
		if !ok {
			break
		}
		var done []sts.Sent
		for _, part := range bin.Parts {
			fc := part.File.(*sendableChunk)
			fc.setStarted(bin.GetStarted())
			if fc.addSent(part.End - part.Beg) {
				fc.setCompleted(bin.GetCompleted())
				_, len := fc.GetSlice()
				if part.Beg == 0 && part.End == len {
					// If the file fit entirely in a bin, we can compute a more
					// precise send time.
					n := bin.GetCompleted().Sub(bin.GetStarted()).Nanoseconds()
					r := float64(0)
					if bin.Bytes > bin.BytesLeft {
						r = float64(part.End-part.Beg) / float64(bin.Bytes-bin.BytesLeft)
					}
					d := time.Duration(float64(n) * r)
					fc.setCompleted(bin.GetStarted().Add(d))
					// ms = int64((r * float64(d.Nanoseconds())) / 1e6)
				}
				// logging.Debug("SEND Sent:", f.GetRelPath(), f.GetSize(), f.GetStarted(), f.GetCompleted())
				// logging.Sent(f.GetRelPath(), f.GetHash(), f.GetSize(), ms, sender.conf.TargetName)
				done = append(done, fc)
			}
		}
		s := bin.GetCompleted().Sub(bin.GetStarted()).Seconds()
		mb := float64(bin.Bytes-bin.BytesLeft) / float64(1024) / float64(1024)
		logging.Info(fmt.Sprintf("SEND Bin Throughput: %3d part(s), %10.2f MB, %6.2f sec, %6.2f MB/sec", len(bin.Parts), mb, s, mb/s))
		sender.addStats(bin.GetCompleted().Sub(bin.GetStarted()), bin.Bytes-bin.BytesLeft)
		if len(done) > 0 {
			sender.done <- done
		}
	}
}

// sendBin sends a bin in a loop to handle errors and keep trying.
func (sender *Sender) send(bin *bin.Bin) {
	nerr := 0
	for {
		bin.SetStarted()
		n, err := sender.sendFunc(bin)
		if err == nil {
			break
		}
		// If somehow part of the bin was successful, let's only resend where we
		// left off.
		if n > 0 && n < len(bin.Parts) {
			b := bin.Split(n)
			bin.SetCompleted()
			sender.doneBin <- bin
			bin = b
		}
		nerr++
		logging.Error("Bin send failed:", err.Error())
		time.Sleep(time.Duration(nerr) * time.Second) // Wait longer the more it fails.
	}
	bin.SetCompleted()
	sender.doneBin <- bin
}

func (sender *Sender) addStats(duration time.Duration, bytes int64) {
	sender.statLock.Lock()
	defer sender.statLock.Unlock()
	if sender.statSince.IsZero() {
		sender.statSince = time.Now()
		return
	}
	sender.bytesOut += bytes
	sender.bytesPerSecCount++
	sender.bytesPerSecSum += float64(bytes) / duration.Seconds()
	d := time.Now().Sub(sender.statSince)
	if d > sender.conf.StatInterval {
		s := d.Seconds()
		mb := float64(sender.bytesOut) / float64(1024) / float64(1024)
		avg := (sender.bytesPerSecSum / float64(sender.bytesPerSecCount)) / float64(1024) / float64(1024)
		logging.Info(fmt.Sprintf("SEND Throughput: %.2fMB, %.2fs, %.2f MB/s", mb, s, mb/s))
		logging.Info(
			fmt.Sprintf(
				"SEND Throughput: %.2f MB/s * %d threads = %.2f MB/s",
				avg,
				sender.conf.Threads,
				avg*float64(sender.conf.Threads)))
		sender.bytesOut = 0
		sender.bytesPerSecCount = 0
		sender.bytesPerSecSum = 0
		sender.statSince = time.Now()
	}
}
