package reflectutil

import (
	"reflect"
	"testing"
)

type copyValue struct {
	Name   string
	Count  int
	Labels []string
	hidden string
}

func TestCopyStruct(t *testing.T) {
	target := copyValue{Name: "kept", hidden: "target"}
	source := copyValue{
		Name:   "source",
		Count:  42,
		Labels: []string{"one", "two"},
		hidden: "source",
	}

	CopyStruct(&target, &source)

	if target.Name != "kept" {
		t.Fatalf("Name = %q, want existing value", target.Name)
	}
	if target.Count != 42 {
		t.Fatalf("Count = %d, want 42", target.Count)
	}
	if !reflect.DeepEqual(target.Labels, source.Labels) {
		t.Fatalf("Labels = %#v, want %#v", target.Labels, source.Labels)
	}
	if target.hidden != "target" {
		t.Fatalf("hidden = %q, want unexported field unchanged", target.hidden)
	}
}

func TestCopyStructIgnoresUnsupportedValues(t *testing.T) {
	target := copyValue{}
	CopyStruct(target, copyValue{Count: 1})
	CopyStruct(&target, &struct{ Count int }{Count: 2})
	CopyStruct("target", "source")

	if target.Count != 0 {
		t.Fatalf("Count = %d, want unchanged", target.Count)
	}
}

func TestIsZero(t *testing.T) {
	zero := 0
	nonzero := 1
	tests := []struct {
		name  string
		value interface{}
		want  bool
	}{
		{name: "zero int", value: 0, want: true},
		{name: "nonzero int", value: 1, want: false},
		{name: "empty string", value: "", want: true},
		{name: "nil slice", value: []string(nil), want: true},
		{name: "empty nonnil slice", value: []string{}, want: false},
		{name: "nil map", value: map[string]int(nil), want: true},
		{name: "empty nonnil map", value: map[string]int{}, want: false},
		{name: "nil function", value: (func())(nil), want: true},
		{name: "zero array", value: [2]int{}, want: true},
		{name: "nonzero array", value: [2]int{0, 1}, want: false},
		{name: "nil pointer", value: (*int)(nil), want: true},
		{name: "pointer to zero", value: &zero, want: true},
		{name: "pointer to nonzero", value: &nonzero, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsZero(reflect.ValueOf(test.value)); got != test.want {
				t.Fatalf("IsZero(%#v) = %t, want %t", test.value, got, test.want)
			}
		})
	}
}

func TestIsZeroStruct(t *testing.T) {
	type value struct {
		Count int
		Name  string
	}

	zero := value{}
	if !IsZero(reflect.ValueOf(&zero).Elem()) {
		t.Fatal("IsZero(zero struct) = false, want true")
	}

	nonzero := value{Name: "set"}
	if IsZero(reflect.ValueOf(&nonzero).Elem()) {
		t.Fatal("IsZero(nonzero struct) = true, want false")
	}

	if IsZero(reflect.ValueOf(value{})) {
		t.Fatal("IsZero(non-settable struct) = true, want false")
	}
}
