package util

import (
	"io"
	"os"
	"testing"
)

// Tests if logger can write logs to disk.
func TestLogging(t *testing.T) {
	old_dir, _ := os.Getwd()
	os.Chdir("../../test")
	send_logger := NewLogger("logs", LOGGING_SEND)
	receiver_logger := NewLogger("logs", LOGGING_RECEIVE)
	error_logger := NewLogger("logs", LOGGING_ERROR)
	send_logger.LogSend("foo/bar.cdf", "foobar", int64(45), "localhost", 5000)
	receiver_logger.LogReceive("foo/bar.cdf", "foobar", int64(45), "localhost")
	error_logger.LogError(io.EOF.Error(), 20)
	error_size, _ := os.Stat(error_logger.log_path)
	receiver_size, _ := os.Stat(receiver_logger.log_path)
	sender_size, _ := os.Stat(send_logger.log_path)
	if error_size.Size() > 0 && receiver_size.Size() > 0 && sender_size.Size() > 0 {
		// Log files exist, test passes
	} else {
		t.Error("Nonexistant or empty log files!")
	}
	os.RemoveAll("logs")
	os.Chdir(old_dir)
}
