package main

import (
    "os"
    "testing"
    "time"
    "util"
)

var cache *Cache
var bin_channel chan Bin

// TestLoadSaveBins tests whether a bin will save and load correctly.
func TestLoadSaveBins(t *testing.T) {
    // Make sure there are no bins in the bins directory when running this test, or it will fail.
    defer os.Remove(util.JoinPath("bins", "d751713988987e9331980363e24189ce.bin"))
    bin_channel = make(chan Bin, 5)
    timeout_channel := make(chan bool, 1)
    cwd, _ := os.Getwd()
    cache = NewCache("cache_test.dat", cwd, 3000, bin_channel)
    // Create and save new bin
    dummy_bin := NewBin(3000, cwd)
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
    defer os.Remove(util.JoinPath("bins", "efade2dfc36eb815d20241693bbdb6f2.bin"))
    defer os.Remove(util.JoinPath("bins", "bceb29582e576c0bcb92850ff2544b08.bin"))
    fake_queue := make(chan Bin, 5)
    timeout_channel := make(chan bool, 1)
    fake_sender := NewSender(fake_queue, false)
    senders := make([]*Sender, 1)
    senders[0] = fake_sender
    cache.SetSenders(senders)
    // Test a bin filling up with two files
    cache.listener.Files["send_test.txt"] = 0
    cache.listener.Files["send_test2.txt"] = 0
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
            if sent_bin.Files[0].TotalSize == 60 || sent_bin.Files[0].TotalSize == 65 {
                // Data in bin was correctly set
            } else {
                t.Errorf("Bin data not set correctly. Expected total size 60, got %d", sent_bin.Files[0].TotalSize)
                return
            }
        } else {
            t.Errorf("Expected bin with 2 files after allocation, got Bin with %d", len(sent_bin.Files))
        }
    }
}
