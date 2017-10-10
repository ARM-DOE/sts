package cache

import (
	"fmt"
	"os"
	"testing"
	"time"

	"code.arm.gov/dataflow/sts"
	"code.arm.gov/dataflow/sts/fileutil"
	"code.arm.gov/dataflow/sts/mock"
)

var root = "/var/tmp/sts/cache"

func tearDown() {
	os.RemoveAll(root)
}

func TestJSON(t *testing.T) {
	tearDown()
	os.MkdirAll(root, 0775)
	cache, err := NewJSON(root, "")
	if err != nil {
		t.Fatal(err)
	}
	n := 1000
	filePath := "/an/example/root/plus/name"
	fileName := "plus/name"
	var names []string
	for i := 0; i < n; i++ {
		names = append(names, fmt.Sprintf("%s-%d", fileName, i))
		cache.Add(&mock.File{
			Path: filePath,
			Name: names[i],
			Size: 1024 * int64(i),
			Time: time.Now(),
			Hash: fileutil.StringMD5(names[i]),
		})
	}
	if err = cache.Persist(); err != nil {
		t.Fatal(err)
	}
	cache, err = NewJSON(root, "")
	if err != nil {
		t.Fatal(err)
	}
	cache.Done("bogus")
	cache.Remove("bogus")
	for i := 0; i < n; i++ {
		if cache.Get(names[i]) == nil {
			t.Fatal("Should exist:", names[i])
		}
		done := func(name string) {
			cache.Done(name)
		}
		go done(names[i])
		go done(names[i])
	}
	replace := &mock.File{
		Path: filePath,
		Name: names[0],
		Size: 1024 * 2,
		Time: time.Now(),
		Hash: fileutil.StringMD5(filePath),
	}
	cache.Add(replace)
	replaced := cache.Get(names[0])
	if replaced.GetSize() == 0 {
		t.Fatal("Cache didn't update")
	}
	i := 0
	cache.Iterate(func(f sts.Cached) bool {
		i++
		if i > n/2 {
			return true
		}
		return false
	})
	if i <= n/2 {
		t.Fatal("Iterate failed")
	}
	cache.Iterate(func(f sts.Cached) bool {
		cache.Remove(f.GetRelPath())
		return false
	})
}
