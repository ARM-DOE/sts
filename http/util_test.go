package http

import (
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*stdhttp.Request) (*stdhttp.Response, error)

func (f roundTripFunc) RoundTrip(req *stdhttp.Request) (*stdhttp.Response, error) {
	return f(req)
}

func TestBandwidthMonitorCountsRequestAndBytes(t *testing.T) {
	monitor := &bandwidthMonitorTransport{transport: roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
		_, _ = io.ReadAll(req.Body)
		return &stdhttp.Response{
			StatusCode: stdhttp.StatusOK,
			Body:       io.NopCloser(strings.NewReader("reply")),
		}, nil
	})}
	req, err := stdhttp.NewRequest(stdhttp.MethodPut, "http://example.test", strings.NewReader("request"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := monitor.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if monitor.GetRequests() != 1 || monitor.GetBytesSent() != 7 || monitor.GetBytesReceived() != 5 {
		t.Fatalf("unexpected counters: requests=%d sent=%d received=%d", monitor.GetRequests(), monitor.GetBytesSent(), monitor.GetBytesReceived())
	}
}
