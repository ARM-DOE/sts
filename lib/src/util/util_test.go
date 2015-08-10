package util

import "testing"

func TestGenerateMD5(t *testing.T) {
	test := []struct {
		data   []byte
		result string
	}{
		{[]byte("hello, World!"), "fcff297b5772aa6d04967352c5f4eb96"},
		{[]byte("site transfer test"), "f4a6f6a36cb06b0226020547cef37cd4"},
		{[]byte("hello, Gopher"), "fd819b6615290f263c9b926fe1c9f635"},
		{[]byte("go is a fun language"), "a8c67b0696cee1717c9c4d11e658048c"},
		{[]byte("testing is always needed"), "9ff800f8bbe6997d143ff967d28e1dd7"},
	}

	for _, c := range test {
		got := GenerateMD5(c.data)
		if got != c.result {
			t.Errorf("Generate MD5(%q) == %q, want %q", c.data, got, c.result)
		}
	}
}

func TestIsStringInArray(t *testing.T) {
	testArray := []string{
		"hello, world",
		"site transfer test",
		"hello, Gopher",
		"go is a fun language",
		"testing is always needed",
	}
	test := []struct {
		value  string
		result bool
	}{
		{"hello, world", true},
		{"hello", false},
		{"testing is always needed", true},
		{"testing is always needd", false},
		{"goodbye", false},
	}
	for _, c := range test {
		got := IsStringInArray(testArray, c.value)
		if got != c.result {
			t.Errorf("IsStringInArray(%q) == %q, want %q", c.value, got, c.result)
		}
	}
}

func TestIsIntInArray(t *testing.T) {
	testArray := []int{1, 2, 3, 4, 10, 20, 50, 70, 100, 2000}
	test := []struct {
		value  int
		result bool
	}{
		{1, true},
		{2, true},
		{70, true},
		{22, false},
		{7, false},
		{300, false},
	}

	for _, c := range test {
		got := IsIntInArray(testArray, c.value)
		if got != c.result {
			t.Errorf("IsIntInArray(%q) == %q, want %q", c.value, got, c.result)
		}
	}
}
