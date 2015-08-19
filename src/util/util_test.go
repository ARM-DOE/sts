package util

import (
    "testing"
)

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
