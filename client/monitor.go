package client

import (
	"fmt"
	"sync"
	"time"

	"code.arm.gov/dataflow/sts/log"
)

type throughput struct {
	bytes     int64
	nanoStart int64
	nanoStop  int64
}

type throughputMonitor struct {
	output      []throughput
	mux         sync.RWMutex
	logTime     time.Time
	logInterval time.Duration
}

const maxOutputs = 1000

func (tm *throughputMonitor) add(bytes int64, start, stop time.Time) {
	tm.mux.Lock()
	defer tm.mux.Unlock()
	tm.output = append(tm.output, throughput{
		bytes,
		start.UnixNano(),
		stop.UnixNano(),
	})
	usesLog := tm.logInterval > time.Duration(0)
	count := len(tm.output)
	if count > maxOutputs && (!usesLog || tm.output[0].nanoStart < tm.logTime.UnixNano()) {
		tm.output = tm.output[count-maxOutputs:]
	}
	if usesLog {
		if tm.logTime.IsZero() {
			tm.logTime = start
		}
		since := stop.Sub(tm.logTime)
		if since > tm.logInterval {
			t := tm.logTime.UnixNano()
			output := []throughput{{0, 0, t}}
			for i, out := range tm.output {
				if out.nanoStop >= t {
					output = tm.output[i:]
					break
				}
			}
			bytes, seconds, pct := compute(output)
			mb := float64(bytes) / float64(1024) / float64(1024)
			log.Info(
				fmt.Sprintf(
					"TOTAL Throughput: %.2fMB, %.2fs, %.2f MB/s (%d%% idle)",
					mb, seconds, mb/seconds, int(pct),
				),
			)
			tm.logTime = time.Now()
		}
		return
	}
}

func compute(output []throughput) (bytes int64, seconds, pct float64) {
	if len(output) == 0 {
		return
	}
	var t int64
	duration := output[len(output)-1].nanoStop - output[0].nanoStart
	active := duration
	// Attempt to remove times where nothing was being sent
	for i, r := range output[1:] {
		// i is actually the real index minus 1 since we are slicing at 1
		t = output[i].nanoStop
		if r.nanoStart > t {
			active -= (r.nanoStart - t)
		}
		bytes += r.bytes
	}
	seconds = time.Duration(time.Nanosecond * time.Duration(active)).Seconds()
	pct = float64(100) * float64(duration-active) / float64(duration)
	return
}

func (tm *throughputMonitor) compute() (bytes int64, seconds, pct float64) {
	tm.mux.RLock()
	defer tm.mux.RUnlock()
	bytes, seconds, pct = compute(tm.output)
	return
}

// if broker.statSince.IsZero() {
//     broker.statSince = payload.GetStarted()
// }
// broker.sendTimes = append(broker.sendTimes, [2]int64{
//     payload.GetStarted().UnixNano(),
//     payload.GetCompleted().UnixNano(),
// })
// broker.bytesOut += payload.GetSize()
// d := payload.GetCompleted().Sub(broker.statSince)
// if d > broker.Conf.StatInterval {
//     // Attempt to remove times where nothing was being sent
//     var t int64
//     active := d
//     ranges := append([][2]int64{{
//         int64(0),
//         broker.statSince.UnixNano(),
//     }}, broker.sendTimes...)
//     for i, r := range ranges[1:] {
//         // i is actually the real index minus 1 since we are slicing at 1
//         t = ranges[i][1]
//         if r[0] > t {
//             active -= time.Nanosecond * time.Duration(r[0]-t)
//         }
//     }
//     mb := float64(broker.bytesOut) / float64(1024) / float64(1024)
//     s := active.Seconds()
//     throughput := fmt.Sprintf(
//         "TOTAL Throughput: %.2fMB, %.2fs, %.2f MB/s (%d%% idle)",
//         mb, s, mb/s, int(100*(d-active)/d))
//     broker.setState(map[string]interface{}{"Throughput": throughput})
//     log.Info(throughput)
//     broker.bytesOut = 0
//     broker.statSince = time.Now()
//     broker.sendTimes = nil
// }
