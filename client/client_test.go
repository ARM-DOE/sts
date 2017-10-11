package client

import (
	"fmt"
	"testing"

	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
)

func TestScan(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})
	n := 100
	broker := &Broker{
		Conf: &Conf{
			Store: mock.NewStore(n, 1024*100),
			Cache: mock.NewCache(),
		},
	}
	hashed := broker.scan()
	if len(hashed) != n {
		t.Error(fmt.Sprintf("Expected %d but got %d", n, len(hashed)))
	}
}
