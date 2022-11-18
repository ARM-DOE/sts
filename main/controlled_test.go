package main

import (
	"testing"
)

func TestMachineID(t *testing.T) {
	id0, err := getMachineID(true)
	if err != nil {
		t.Fatal(err)
	}
	id1, err := getMachineID(false)
	if err != nil {
		t.Fatal(err)
	}
	if id0 != id1 {
		t.Fatalf("%s != %s", id0, id1)
	}
}
