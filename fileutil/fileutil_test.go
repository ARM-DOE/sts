package fileutil

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type readSeekCloser struct {
	*strings.Reader
}

func (readSeekCloser) Close() error { return nil }

func TestInitPath(t *testing.T) {
	if _, err := InitPath("", "relative", true); err == nil {
		t.Fatal("InitPath with relative path and empty root succeeded, want error")
	}

	root := t.TempDir()
	dir, err := InitPath(root, filepath.Join("nested", "dir"), true)
	if err != nil {
		t.Fatalf("InitPath(directory): %v", err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("Stat(%s) = %#v, %v; want directory", dir, info, err)
	}

	file, err := InitPath(root, filepath.Join("another", "file.txt"), false)
	if err != nil {
		t.Fatalf("InitPath(file): %v", err)
	}
	if _, err := os.Stat(filepath.Dir(file)); err != nil {
		t.Fatalf("Stat(parent): %v", err)
	}
}

func TestFindLineAndGuessCompressed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lines.txt")
	if err := os.WriteFile(path, []byte("first\nmatching line\nlast\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got := FindLine(path, []byte("match")); got != "matching line" {
		t.Fatalf("FindLine = %q, want matching line", got)
	}
	if got := FindLine(path, []byte("missing")); got != "" {
		t.Fatalf("FindLine(missing) = %q, want empty", got)
	}
	if got := FindLine(filepath.Join(t.TempDir(), "missing"), nil); got != "" {
		t.Fatalf("FindLine(missing file) = %q, want empty", got)
	}

	for _, test := range []struct {
		path string
		want bool
	}{
		{path: "archive.tar.gz", want: true},
		{path: "archive.GZ", want: false},
		{path: "archive.gzip", want: false},
		{path: "", want: false},
	} {
		if got := GuessCompressed(test.path); got != test.want {
			t.Errorf("GuessCompressed(%q) = %t, want %t", test.path, got, test.want)
		}
	}
}

func TestJSONFiles(t *testing.T) {
	type document struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	want := document{Name: "test", Count: 3}
	root := t.TempDir()

	compact := filepath.Join(root, "compact.json")
	if err := WriteJSON(compact, want); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	var got document
	if err := LoadJSON(compact, &got); err != nil {
		t.Fatalf("LoadJSON: %v", err)
	}
	if got != want {
		t.Fatalf("LoadJSON = %#v, want %#v", got, want)
	}
	if _, err := os.Stat(compact + LockExt); !os.IsNotExist(err) {
		t.Fatalf("lock file remains after WriteJSON: %v", err)
	}

	human := filepath.Join(root, "human.json")
	if err := WriteHumanJSON(human, want); err != nil {
		t.Fatalf("WriteHumanJSON: %v", err)
	}
	encoded, err := os.ReadFile(human)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(encoded), "\n    \"name\"") {
		t.Fatalf("WriteHumanJSON output is not indented: %q", encoded)
	}

	if err := LoadJSON(filepath.Join(root, "missing.json"), &got); err == nil {
		t.Fatal("LoadJSON(missing) succeeded, want error")
	}
	invalid := filepath.Join(root, "invalid.json")
	if err := os.WriteFile(invalid, []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile(invalid): %v", err)
	}
	if err := LoadJSON(invalid, &got); err == nil {
		t.Fatal("LoadJSON(invalid) succeeded, want error")
	}
}

func TestMD5Helpers(t *testing.T) {
	const contents = "abcdef"
	const wantFull = "e80b5017098950fc58aad83c8c14978e"
	if got := StringMD5(contents); got != wantFull {
		t.Fatalf("StringMD5 = %q, want %q", got, wantFull)
	}

	path := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if got, err := FileMD5(path); err != nil || got != wantFull {
		t.Fatalf("FileMD5 = %q, %v; want %q", got, err, wantFull)
	}
	reader := readSeekCloser{Reader: strings.NewReader(contents)}
	if got, err := ReadableMD5(reader); err != nil || got != wantFull {
		t.Fatalf("ReadableMD5 = %q, %v; want %q", got, err, wantFull)
	}
	if got, err := PartialMD5(path, 1, 4); err != nil || got != StringMD5("bcd") {
		t.Fatalf("PartialMD5 = %q, %v; want hash of bcd", got, err)
	}
	if _, err := FileMD5(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("FileMD5(missing) succeeded, want error")
	}
}

func TestCopyMoveAndReaddir(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.txt")
	copyPath := filepath.Join(root, "copy.txt")
	moved := filepath.Join(root, "moved.txt")
	if err := os.WriteFile(source, []byte("contents"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := Copy(source, copyPath); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := Move(copyPath, moved); err != nil {
		t.Fatalf("Move: %v", err)
	}
	if _, err := os.Stat(copyPath); !os.IsNotExist(err) {
		t.Fatalf("source remains after Move: %v", err)
	}
	file, err := os.Open(moved)
	if err != nil {
		t.Fatalf("Open(moved): %v", err)
	}
	contents, err := io.ReadAll(file)
	file.Close()
	if err != nil || string(contents) != "contents" {
		t.Fatalf("moved contents = %q, %v", contents, err)
	}

	entries, err := Readdir(root)
	if err != nil {
		t.Fatalf("Readdir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Readdir returned %d entries, want 2", len(entries))
	}
	if _, err := Readdir(filepath.Join(root, "missing")); err == nil {
		t.Fatal("Readdir(missing) succeeded, want error")
	}
}

func TestCombineFuncsOverridesEarlierValues(t *testing.T) {
	first := map[string]interface{}{"value": "first"}
	second := map[string]interface{}{"value": "second", "other": 2}
	combined := CombineFuncs(first, nil, second)
	if combined["value"] != "second" || combined["other"] != 2 {
		t.Fatalf("CombineFuncs = %#v", combined)
	}
}
