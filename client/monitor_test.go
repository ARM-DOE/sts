package client

import (
	"fmt"
	"testing"
	"time"
)

func fmtThroughput(bytes int64, seconds, pct float64) string {
	return fmt.Sprintf(
		"%d, %.2fs, %.2f b/s (%d%% idle)",
		bytes,
		seconds,
		float64(bytes)/float64(seconds),
		int(pct),
	)
}

func verify(t *testing.T, outputs []throughput, expected string) {
	bytes, seconds, pct := compute(outputs)
	got := fmtThroughput(bytes, seconds, pct)
	if got != expected {
		t.Fatalf("got %s, expected %s", got, expected)
	}
}

func TestComputeThroughput(t *testing.T) {
	var i int
	var outputs []throughput
	start := time.Now().Add(time.Second * -10)
	for i = 0; i < 10; i++ {
		outputs = append(outputs, throughput{
			10,
			start.Add(time.Second * time.Duration(i)).UnixNano(),
			start.Add(time.Second * time.Duration(i+1)).UnixNano(),
		})
	}
	verify(t, outputs, "100, 10.00s, 10.00 b/s (0% idle)")
	outputs = nil
	for i = 0; i < 10; i++ {
		outputs = append(outputs, throughput{
			100,
			start.Add(time.Second * time.Duration(i)).UnixNano(),
			start.Add(time.Second * time.Duration(i+3)).UnixNano(),
		})
	}
	verify(t, outputs, "1000, 12.00s, 83.33 b/s (0% idle)")
	outputs = nil
	for i = 0; i < 10; i++ {
		s := start.Add(time.Second * time.Duration(i))
		outputs = append(outputs, throughput{
			5,
			s.UnixNano(),
			s.Add(time.Millisecond * 500).UnixNano(),
		})
	}
	verify(t, outputs, "50, 5.00s, 10.00 b/s (47% idle)")
}
