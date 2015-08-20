package main

import (
    "os"
    "testing"
    "time"
)

var cache *Cache
var bin_channel chan Bin

// TestLoadSaveBins tests whether a bin will save and load correctly.
func TestLoadSaveBins(t *testing.T) {
    defer os.Remove("bins" + string(os.PathSeparator) + "d751713988987e9331980363e24189ce.bin")
    bin_channel = make(chan Bin, 5)
    timeout_channel := make(chan bool, 1)
    cache = CacheFactory("cache_test.dat", "test_dir", bin_channel, 3000)
    // Create and save new bin
    dummy_bin := BinFactory(3000, "test_dir")
    dummy_bin.BytesLeft = -66
    dummy_bin.save()
    cache.loadBins()
    go func() {
        time.Sleep(200 * time.Millisecond)
        timeout_channel <- true
    }()
    select {
    case <-timeout_channel:
        t.Error("Bin was not loaded within timeout time")
        return
    case sent_bin := <-bin_channel:
        if sent_bin.BytesLeft == -66 {
            // Data saved and loaded correctly.
        }
    }
}

// TestMultiAllocate tests whether the cache will allocate multiple files to one bin.
func TestMultiAllocate(t *testing.T) {
    defer os.Remove("cache_test.dat")
    defer os.Remove("bins" + string(os.PathSeparator) + "b5c3a78acc1c336479430636f676746e.bin")
    defer os.Remove("bins" + string(os.PathSeparator) + "82f9219e048a5caf3f268dae105b151d.bin")
    fake_queue := make(chan Bin, 5)
    timeout_channel := make(chan bool, 1)
    fake_sender := SenderFactory(fake_queue, false)
    senders := make([]*Sender, 1)
    senders[0] = fake_sender
    cache.SetSenders(senders)
    // Test a bin filling up with two files
    cache.listener.Files["test_dir/send_test.txt"] = 0
    cache.listener.Files["test_dir/send_test2.txt"] = 0
    cache.allocate()
    go func() {
        time.Sleep(200 * time.Millisecond)
        timeout_channel <- true
    }()
    select {
    case <-timeout_channel:
        t.Error("Bin allocation did not complete within timeout")
        return
    case sent_bin := <-bin_channel:
        if len(sent_bin.Files) == 2 {
            // Bin contains two files
            if sent_bin.Files[0].TotalSize == 60 {
                // Data in bin was correctly set
            } else {
                t.Errorf("Bin data not set correctly. Expected total size 60, got ", sent_bin.Files[0].TotalSize)
            }
        } else {
            t.Errorf("Expected bin with 2 files after allocation, got Bin with %d", len(sent_bin.Files))
        }
    }
}
