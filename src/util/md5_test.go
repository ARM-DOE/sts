package util

import (
	"os"
	"testing"
)

var test = []struct {
	data   []byte
	result string
}{
	{[]byte("hello, World!"), "fcff297b5772aa6d04967352c5f4eb96"},
	{[]byte("site transfer test"), "f4a6f6a36cb06b0226020547cef37cd4"},
	{[]byte("hello, Gopher"), "fd819b6615290f263c9b926fe1c9f635"},
	{[]byte("go is a fun language"), "a8c67b0696cee1717c9c4d11e658048c"},
	{[]byte("testing is always needed"), "9ff800f8bbe6997d143ff967d28e1dd7"},
}

func TestSetup(t *testing.T) {
	os.Chdir("../../test")
}

func TestGenerateMD5(t *testing.T) {
	for _, c := range test {
		got := GenerateMD5(c.data)
		if got != c.result {
			t.Errorf("Generate MD5(%s) == %s, want %s", c.data, got, c.result)
		}
	}
}

func TestUpdate(t *testing.T) {
	for _, v := range test {
		test_stream := NewStreamMD5()
		test_stream.Update(v.data)
		result := test_stream.SumString()
		if result != v.result {
			t.Errorf("StreamMD5.Update(%s) expected %s got %s", v.data, v.result, result)
		}
	}
}

func TestReadAll(t *testing.T) {
	var test_files = []struct {
		name   string
		result string
	}{
		{"empty.txt", "d41d8cd98f00b204e9800998ecf8427e"},
		{"small.txt", "5eb63bbbe01eeed093cb22bb8f5acdc3"},
		{"large.txt", "d65b56fa8ecbe65dfe30511eab4a4789"},
	}
	for _, v := range test_files {
		md5_stream := NewStreamMD5()
		fi, _ := os.Open(JoinPath("test_files", v.name))
		result := md5_stream.ReadAll(fi)
		if result != v.result {
			t.Errorf("StreamMD5.ReadAll(%s) expected %s got %s", v.name, v.result, result)
		}
	}
}
