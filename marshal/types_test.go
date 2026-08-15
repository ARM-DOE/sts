package marshal

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDurationJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{name: "string", input: `"1h2m3s"`, want: time.Hour + 2*time.Minute + 3*time.Second},
		{name: "number", input: `1500`, want: 1500 * time.Nanosecond},
		{name: "zero", input: `"0s"`, want: 0},
		{name: "invalid string", input: `"later"`, wantErr: true},
		{name: "invalid type", input: `null`, wantErr: true},
		{name: "invalid json", input: `{`, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got Duration
			err := json.Unmarshal([]byte(test.input), &got)
			if test.wantErr {
				if err == nil {
					t.Fatalf("Unmarshal(%s) succeeded, want error", test.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal(%s): %v", test.input, err)
			}
			if got.Duration != test.want {
				t.Fatalf("Unmarshal(%s) = %s, want %s", test.input, got.Duration, test.want)
			}
		})
	}

	encoded, err := json.Marshal(Duration{Duration: 90 * time.Second})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(encoded) != `"1m30s"` {
		t.Fatalf("Marshal = %s, want %q", encoded, "1m30s")
	}
}

func TestNanoTimeJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{name: "nanoseconds", input: `"123+456789"`, want: time.Unix(123, 456789)},
		{name: "legacy seconds", input: `123`, want: time.Unix(123, 0)},
		{name: "negative seconds", input: `"-2+500"`, want: time.Unix(-2, 500)},
		{name: "missing separator", input: `"123"`, wantErr: true},
		{name: "too many fields", input: `"1+2+3"`, wantErr: true},
		{name: "invalid seconds", input: `"x+2"`, wantErr: true},
		{name: "invalid nanoseconds", input: `"1+x"`, wantErr: true},
		{name: "invalid type", input: `null`, wantErr: true},
		{name: "invalid json", input: `{`, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got NanoTime
			err := json.Unmarshal([]byte(test.input), &got)
			if test.wantErr {
				if err == nil {
					t.Fatalf("Unmarshal(%s) succeeded, want error", test.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal(%s): %v", test.input, err)
			}
			if !got.Time.Equal(test.want) {
				t.Fatalf("Unmarshal(%s) = %s, want %s", test.input, got.Time, test.want)
			}
		})
	}

	want := time.Unix(123, 456789)
	encoded, err := json.Marshal(NanoTime{Time: want})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(encoded) != `"123+456789"` {
		t.Fatalf("Marshal = %s, want %q", encoded, "123+456789")
	}
}
