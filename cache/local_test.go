package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/fileutil"
	"github.com/arm-doe/sts/mock"
)

var root = "/var/tmp/sts/cache"

func tearDown() {
	os.RemoveAll(root)
}

func TestJSON(t *testing.T) {
	tearDown()
	_ = os.MkdirAll(root, 0775)
	rootPath := "/an/example/root"
	cache, err := NewJSON(root, rootPath, "")
	if err != nil {
		t.Fatal(err)
	}
	n := 1000
	fileName := "plus/name"
	filePath := filepath.Join(rootPath, fileName)
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
	cache, err = NewJSON(root, rootPath, "")
	if err != nil {
		t.Fatal(err)
	}
	cache.Done("bogus", nil)
	cache.Remove("bogus")
	for i := 0; i < n; i++ {
		if cache.Get(names[i]) == nil {
			t.Fatal("Should exist:", names[i])
		}
		done := func(name string) {
			cache.Done(name, nil)
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
		return i > n/2
	})
	if i <= n/2 {
		t.Fatal("Iterate failed")
	}
	cache.Iterate(func(f sts.Cached) bool {
		cache.Remove(f.GetName())
		return false
	})
}
