package store

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/arm-doe/sts"
	"github.com/arm-doe/sts/log"
	"github.com/arm-doe/sts/mock"
)

func TestMain(m *testing.M) {
	log.InitExternal(&mock.Logger{})
	os.Exit(m.Run())
}

func writeOldFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("Chtimes(%s): %v", path, err)
	}
}

func TestMetaEncoding(t *testing.T) {
	want := &meta{Link: `/tmp/a "quoted" path`}
	encoded, err := encodeMeta(want)
	if err != nil {
		t.Fatalf("encodeMeta: %v", err)
	}
	got, err := decodeMeta(encoded)
	if err != nil {
		t.Fatalf("decodeMeta: %v", err)
	}
	if got.Link != want.Link {
		t.Fatalf("Link = %q, want %q", got.Link, want.Link)
	}
	if _, err := decodeMeta([]byte(`{"link":`)); err == nil {
		t.Fatal("decodeMeta(invalid JSON) succeeded, want error")
	}
}

func TestLocalScanFiltersFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	writeOldFile(t, filepath.Join(root, "keep.txt"), "keep")
	writeOldFile(t, filepath.Join(root, "skip.log"), "skip")
	writeOldFile(t, filepath.Join(root, ".hidden.txt"), "hidden")
	writeOldFile(t, filepath.Join(root, "sub", "nested.txt"), "nested")

	dir := &Local{
		Root:    root + string(os.PathSeparator),
		MinAge:  time.Hour,
		Include: []*regexp.Regexp{regexp.MustCompile(`\.txt$`)},
		Ignore:  []*regexp.Regexp{regexp.MustCompile(`^sub$`)},
	}
	dir.AddStandardIgnore()
	files, cutoff, err := dir.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(files) != 1 || files[0].GetName() != "keep.txt" {
		t.Fatalf("Scan files = %#v, want keep.txt", files)
	}
	if files[0].GetSize() != 4 || files[0].GetMeta() != nil {
		t.Fatalf("unexpected file metadata: size=%d meta=%q", files[0].GetSize(), files[0].GetMeta())
	}
	if cutoff.IsZero() || cutoff.After(time.Now().Add(-59*time.Minute)) {
		t.Fatalf("cutoff = %s, want approximately one hour ago", cutoff)
	}
	if !dir.ShouldIgnore(&localFile{name: ".another.txt"}) {
		t.Fatal("ShouldIgnore(hidden file) = false, want true")
	}
}

func TestLocalScanAllowAndDisabled(t *testing.T) {
	root := t.TempDir()
	writeOldFile(t, filepath.Join(root, "allow.txt"), "allowed")
	writeOldFile(t, filepath.Join(root, "deny.txt"), "denied")

	dir := &Local{Root: root, IncludeHidden: true}
	files, _, err := dir.Scan(func(file sts.File) bool {
		return file.GetName() == "allow.txt"
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(files) != 1 || files[0].GetName() != "allow.txt" {
		t.Fatalf("Scan files = %#v, want allow.txt", files)
	}

	if err := os.WriteFile(filepath.Join(root, disabledName), nil, 0o600); err != nil {
		t.Fatalf("WriteFile(disabled): %v", err)
	}
	files, scanTime, err := dir.Scan(nil)
	if err != nil {
		t.Fatalf("disabled Scan: %v", err)
	}
	if files != nil || !scanTime.IsZero() {
		t.Fatalf("disabled Scan = (%#v, %s), want nil files and zero time", files, scanTime)
	}
}

func TestLocalOpenSyncAndRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.txt")
	writeOldFile(t, path, "contents")
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}
	file, err := newLocalFile(path, "file.txt", info)
	if err != nil {
		t.Fatalf("newLocalFile: %v", err)
	}
	dir := &Local{}

	reader, err := dir.Open(file)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	contents, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(contents) != "contents" {
		t.Fatalf("ReadAll = %q, %v; want contents", contents, err)
	}

	changed, err := dir.Sync(file)
	if err != nil || changed != nil {
		t.Fatalf("Sync(unchanged) = %#v, %v; want nil, nil", changed, err)
	}
	if err := os.WriteFile(path, []byte("changed contents"), 0o600); err != nil {
		t.Fatalf("WriteFile(changed): %v", err)
	}
	changed, err = dir.Sync(file)
	if err != nil || changed == nil {
		t.Fatalf("Sync(changed) = %#v, %v; want changed file", changed, err)
	}

	if err := dir.Remove(file); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := dir.Remove(file); err != nil {
		t.Fatalf("Remove(missing): %v", err)
	}
	_, err = dir.Sync(file)
	if !dir.IsNotExist(err) {
		t.Fatalf("IsNotExist(%v) = false, want true", err)
	}
}

func TestLocalFileSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	link := filepath.Join(root, "link.txt")
	writeOldFile(t, target, "target")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}
	file, err := newLocalFile(link, "link.txt", info)
	if err != nil {
		t.Fatalf("newLocalFile: %v", err)
	}
	if file.GetSize() != int64(len("target")) {
		t.Fatalf("GetSize = %d, want %d", file.GetSize(), len("target"))
	}
	decoded, err := decodeMeta(file.GetMeta())
	if err != nil || decoded.Link != target {
		t.Fatalf("decodeMeta = %#v, %v; want link %q", decoded, err, target)
	}
	reader, err := (&Local{}).Open(file)
	if err != nil {
		t.Fatalf("Open(symlink): %v", err)
	}
	contents, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(contents) != "target" {
		t.Fatalf("ReadAll = %q, %v; want target", contents, err)
	}
}
