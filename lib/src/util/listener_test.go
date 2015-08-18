package util

import (
    "os"
    "strings"
    "testing"
    "time"
)

func TestCacheReadWrite(t *testing.T) {
    cache_file := "test_files" + string(os.PathSeparator) + "test_cache.dat"
    watch_dir := "test_files" + string(os.PathSeparator) + "watch_dir"
    listener := ListenerFactory(cache_file, watch_dir)
    listener.LoadCache()
    listener.last_update = -55
    listener.WriteCache()
    listener.last_update = -45
    listener.LoadCache()
    if listener.Files["__TIMESTAMP__"] != -55 {
        t.Errorf("Cache read/write failed, expected timestamp -55, got %d", listener.Files["__TIMESTAMP__"])
    }
}

func TestScan(t *testing.T) {
    cache_file := "test_files" + string(os.PathSeparator) + "test_cache.dat"
    watch_file := "test_files" + string(os.PathSeparator) + "watch_dir" + string(os.PathSeparator) + "large.txt"
    watch_dir := "test_files" + string(os.PathSeparator) + "watch_dir"
    listener := ListenerFactory(cache_file, watch_dir)
    listener.LoadCache()
    listener.AddIgnored("ignore_me")
    addition_chan := make(chan string, 1)
    timeout := make(chan bool, 1)
    go listener.Listen(addition_chan)
    os.Chtimes(watch_file, time.Now(), time.Now())
    os.Chtimes(watch_dir+string(os.PathSeparator)+"ignore_me", time.Now(), time.Now())
    go func() {
        time.Sleep(5 * time.Second)
        timeout <- true
    }()
    select {
    case <-timeout:
        t.Error("Exceeded timeout while looking for file")
    case file_name := <-addition_chan:
        if strings.HasSuffix(file_name, "ignore_me") { // Test if file is ignored correctly
            t.Error("Picked up ignored file")
        }
        // Listener picked up file
    }
}
