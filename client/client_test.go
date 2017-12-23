package client

import (
	"testing"

	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
)

func TestScan(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	n := 10000
	broker := &Broker{
		Conf: &Conf{
			Store:   mock.NewStore(n, 1024),
			Cache:   mock.NewCache(),
			Threads: 10,
		},
	}
	hashed := broker.scan()
	if len(hashed) != n {
		t.Errorf("Expected %d but got %d", n, len(hashed))
	}
}
