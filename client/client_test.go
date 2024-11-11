package client

import (
	"testing"

	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/mock"
)

func TestScan(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	n := 10000
	broker := &Broker{
		Conf: &Conf{
			Store:   mock.NewStore(n, n*2),
			Cache:   mock.NewCache(),
			Threads: 10,
		},
	}
	hashed := broker.scan()
	if len(hashed) != n {
		t.Errorf("Expected %d but got %d", n, len(hashed))
	}
}
