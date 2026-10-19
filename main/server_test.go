package main

import (
	"crypto/md5"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arm-doe/sts"
)

func TestS3ExporterForSource(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	requests := make(chan string, 10)
	content := []byte("incoming file")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		writer.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(content)))
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)
	filePath := filepath.Join(t.TempDir(), "incoming.txt")
	if err := os.WriteFile(filePath, content, 0600); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name    string
		conf    *sts.ServerConf
		source  string
		wantKey string
	}{
		{
			name:   "legacy default",
			conf:   &sts.ServerConf{S3: &sts.S3{Bucket: "default-bucket", Prefix: "incoming"}},
			source: "client-a", wantKey: "/default-bucket/incoming/nested/file.txt",
		},
		{
			name: "source override",
			conf: &sts.ServerConf{
				S3: &sts.S3{Bucket: "default-bucket", Prefix: "incoming"},
				S3Sources: map[string]*sts.S3{
					"client-b": {Region: "us-east-1", Bucket: "client-b-bucket", Prefix: "raw"},
				},
			},
			source: "client-b", wantKey: "/client-b-bucket/raw/nested/file.txt",
		},
		{
			name: "unmapped source uses default",
			conf: &sts.ServerConf{
				S3:        &sts.S3{Bucket: "default-bucket", Prefix: "incoming"},
				S3Sources: map[string]*sts.S3{"client-b": {Bucket: "client-b-bucket"}},
			},
			source: "client-a", wantKey: "/default-bucket/incoming/nested/file.txt",
		},
		{
			name: "override does not inherit default prefix",
			conf: &sts.ServerConf{
				S3:        &sts.S3{Bucket: "default-bucket", Prefix: "incoming"},
				S3Sources: map[string]*sts.S3{"client-b": {Bucket: "client-b-bucket"}},
			},
			source: "client-b", wantKey: "/client-b-bucket/nested/file.txt",
		},
		{
			name: "override without default and without inherited prefix",
			conf: &sts.ServerConf{
				S3Sources: map[string]*sts.S3{"client-b": {Bucket: "client-b-bucket"}},
			},
			source: "client-b", wantKey: "/client-b-bucket/nested/file.txt",
		},
		{
			name: "unmapped source without default stays local",
			conf: &sts.ServerConf{
				S3Sources: map[string]*sts.S3{"client-b": {Bucket: "client-b-bucket"}},
			},
			source: "client-a",
		},
		{name: "no S3 stays local", conf: &sts.ServerConf{}, source: "client-a"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exporterForSource, err := newS3ExporterForSource(testCase.conf)
			if err != nil {
				t.Fatal(err)
			}
			exporter := exporterForSource(testCase.source)
			if testCase.wantKey == "" {
				if exporter != nil {
					t.Fatal("expected local storage without an S3 exporter")
				}
				return
			}
			if exporter == nil {
				t.Fatal("missing S3 exporter")
			}
			if exporterForSource(testCase.source) != exporter {
				t.Fatal("source exporter was not reused")
			}
			if err := exporter.Upload(filePath, "nested/file.txt"); err != nil {
				t.Fatal(err)
			}
			if got := <-requests; got != testCase.wantKey {
				t.Fatalf("upload destination = %q, want %q", got, testCase.wantKey)
			}
		})
	}
}

func TestS3ExporterForSourceInvalidDestination(t *testing.T) {
	for _, testCase := range []struct {
		name string
		conf *sts.ServerConf
		want string
	}{
		{"missing default bucket", &sts.ServerConf{S3: &sts.S3{}}, "default S3 destination"},
		{"missing source bucket", &sts.ServerConf{S3Sources: map[string]*sts.S3{"client-b": {}}}, `source "client-b"`},
		{"null source destination", &sts.ServerConf{S3Sources: map[string]*sts.S3{"client-b": nil}}, `source "client-b"`},
		{"blank source", &sts.ServerConf{S3Sources: map[string]*sts.S3{" ": {Bucket: "bucket"}}}, "source name"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := newS3ExporterForSource(testCase.conf)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v, want error containing %q", err, testCase.want)
			}
		})
	}
}
