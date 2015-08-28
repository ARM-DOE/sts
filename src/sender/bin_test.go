package main

import (
    "os"
    "testing"
    "util"
)

func TestSetup(t *testing.T) {
    os.Chdir("../../test")
}

// TestShouldAllocate tests whether the Bin.fill() method will insert files into the Bin in the correct order.
func TestShouldAllocate(t *testing.T) {
    defer os.Remove("cache_test2.dat")
    config = util.ParseConfig("test_config.yaml")
    ch := make(chan Bin, 5)
    dummy_cache := NewCache("cache_test2.dat", "test_dir", 3000, ch)
    //dummy_cache.listener.Files["test_dir/radar/data.cdf"] = 0
    dummy_cache.listener.Files["default/data.cdf"] = 0
    dummy_cache.listener.Files["laser/data.cdf"] = 0
    dummy_cache.listener.Files["log/events.log"] = -1
    dummy_cache.listener.Files["log/events2.log"] = 0
    test_bin := NewBin(3000, "test")
    test_bin.fill(dummy_cache)
    defer os.Remove(test_bin.Name)
    data_order := [4]string{"log/events2.log", "laser/data.cdf", "default/data.cdf"}
    for index, part := range test_bin.Files {
        if data_order[index] != part.Path {
            t.Errorf("Bad allocation order at Bin.Files index %d, expected %s, got %s", index, data_order[index], part.Path)
        }
    }
}
