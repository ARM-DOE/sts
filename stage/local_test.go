package stage

import (
	"path/filepath"
	"testing"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/log"
	"code.arm.gov/dataflow/sts/mock"
)

type logger struct {
}

func (l *logger) Parse(
	handler func(name, renamed, hash string, size int64, t time.Time) bool,
	after time.Time,
	before time.Time,
) bool {
	return true
}

func (l *logger) Sent(file string, hash string, size int64, t time.Time) {
}

func (l *logger) Received(file sts.Received) {
}

func (l *logger) WasReceived(relPath, hash string, after time.Time, before time.Time) bool {
	return true
}

type dispatcher struct {
}

func (d *dispatcher) Dispatch(file string, hash string, size int64, t time.Time) {
}

func (d *dispatcher) Send(file string) error {
	return nil
}

func TestWaitLoop(t *testing.T) {
	log.InitExternal(&mock.Logger{DebugMode: true})

	stage := New(
		"test", "/var/tmp/ststest/stage/test", "/var/tmp/ststest/in/test",
		&logger{}, &dispatcher{})

	hierarchy := map[string][]string{
		"a": {"b"},
		"b": {"c"},
		"c": {"d"},
		"d": {"c", "e"},
		"e": {"a"},
	}
	for prev, names := range hierarchy {
		for _, name := range names {
			file := &finalFile{
				path:  filepath.Join(stage.rootDir, name),
				name:  name,
				prev:  prev,
				hash:  "1234567890abcdef",
				size:  100,
				time:  time.Now().Add(-time.Hour),
				state: stateValidated,
			}
			stage.toWait(filepath.Join(stage.rootDir, file.prev), file, 0)
		}
	}

	for _, prev := range []string{"a", "b", "c", "d", "e"} {
		loop := stage.detectWaitLoop(filepath.Join(stage.rootDir, prev))
		if len(loop) == 0 {
			t.Errorf("Wait loop not detected")
		}
	}
}
