package export

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

/* Before running these tests, ensure you have a MinIO server running locally.
docker run -p 9000:9000 -p 9001:9001 \
    -e "MINIO_ROOT_USER=minioadmin" \
    -e "MINIO_ROOT_PASSWORD=minioadmin" \
    quay.io/minio/minio server /data --console-address ":9001"
*/

const (
	testMinIOEndpoint = "http://localhost:9000"
	testMinIORegion   = "us-east-1"
	testBucket        = "test-bucket"
	testPathPrefix    = "test"
	// MinIO default credentials
	testAccessKey = "minioadmin"
	testSecretKey = "minioadmin"
)

// setupMinIOCredentials sets the required environment variables for MinIO testing
func setupMinIOCredentials() {
	os.Setenv("AWS_ACCESS_KEY_ID", testAccessKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", testSecretKey)
	os.Setenv("AWS_REGION", testMinIORegion)
}

func TestNewS3Upload(t *testing.T) {
	// Set credentials for AWS SDK
	setupMinIOCredentials()

	tests := []struct {
		name        string
		region      string
		endpointURL string
		bucket      string
		pathPrefix  string
		wantErr     bool
	}{
		{
			name:        "valid configuration with endpoint",
			region:      testMinIORegion,
			endpointURL: testMinIOEndpoint,
			bucket:      testBucket,
			pathPrefix:  testPathPrefix,
			wantErr:     false,
		},
		{
			name:        "valid configuration without endpoint",
			region:      "us-west-2",
			endpointURL: "",
			bucket:      testBucket,
			pathPrefix:  testPathPrefix,
			wantErr:     false,
		},
		{
			name:        "empty region",
			region:      "",
			endpointURL: testMinIOEndpoint,
			bucket:      testBucket,
			pathPrefix:  testPathPrefix,
			wantErr:     false,
		},
		{
			name:        "empty bucket",
			region:      testMinIORegion,
			endpointURL: testMinIOEndpoint,
			bucket:      "",
			pathPrefix:  testPathPrefix,
			wantErr:     false,
		},
	}

	cfg, err := getConfig()
	if err != nil {
		t.Fatalf("Failed to get AWS config: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3Upload, err := NewS3Upload(cfg, tt.bucket, tt.pathPrefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewS3Upload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if s3Upload == nil {
					t.Error("NewS3Upload() returned nil without error")
					return
				}
				if s3Upload.bucket != tt.bucket {
					t.Errorf("bucket = %v, want %v", s3Upload.bucket, tt.bucket)
				}
				if s3Upload.pathPrefix != tt.pathPrefix {
					t.Errorf("pathPrefix = %v, want %v", s3Upload.pathPrefix, tt.pathPrefix)
				}
				if s3Upload.uploader == nil {
					t.Error("uploader is nil")
				}
				if s3Upload.ctx == nil {
					t.Error("context is nil")
				}
			}
		})
	}
}

func TestS3Upload_Upload(t *testing.T) {
	// Set credentials for AWS SDK
	setupMinIOCredentials()

	// Skip if MinIO is not available
	if !isMinIOAvailable() {
		t.Skip("MinIO is not available at", testMinIOEndpoint)
	}

	// Setup MinIO client and create bucket
	if err := setupTestBucket(); err != nil {
		t.Fatalf("Failed to setup test bucket: %v", err)
	}
	defer cleanupTestBucket()

	// Create test files
	testFiles := createTestFiles(t)
	defer cleanupTestFiles(testFiles)

	cfg, err := getConfig()
	if err != nil {
		t.Fatalf("Failed to get AWS config: %v", err)
	}

	s3Upload, err := NewS3Upload(cfg, testBucket, testPathPrefix)
	if err != nil {
		t.Fatalf("Failed to create S3Upload: %v", err)
	}

	tests := []struct {
		name     string
		filePath string
		fileName string
		wantErr  bool
	}{
		{
			name:     "upload small file",
			filePath: testFiles["small"],
			fileName: "small-test.txt",
			wantErr:  false,
		},
		{
			name:     "upload large file",
			filePath: testFiles["large"],
			fileName: "large-test.txt",
			wantErr:  false,
		},
		{
			name:     "upload non-existent file",
			filePath: "/non/existent/file.txt",
			fileName: "non-existent.txt",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s3Upload.Upload(tt.filePath, tt.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Upload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Verify file was uploaded by trying to download it
				if err := verifyUpload(tt.fileName); err != nil {
					t.Errorf("Failed to verify upload: %v", err)
				}
			}
		})
	}
}

func TestComputeS3MultipartETag(t *testing.T) {
	testFiles := createTestFiles(t)
	defer cleanupTestFiles(testFiles)

	tests := []struct {
		name     string
		filePath string
		partSize int64
		wantErr  bool
	}{
		{
			name:     "small file single part",
			filePath: testFiles["small"],
			partSize: DefaultPartSize,
			wantErr:  false,
		},
		{
			name:     "large file multipart",
			filePath: testFiles["large"],
			partSize: DefaultPartSize,
			wantErr:  false,
		},
		{
			name:     "non-existent file",
			filePath: "/non/existent/file.txt",
			partSize: DefaultPartSize,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			etag, err := computeS3MultipartETag(tt.filePath, tt.partSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("computeS3MultipartETag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if etag == "" {
					t.Error("computeS3MultipartETag() returned empty ETag")
				}
				if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
					t.Errorf("ETag should be quoted, got: %s", etag)
				}
			}
		})
	}
}

// Helper functions

func getConfig() (aws.Config, error) {
	setupMinIOCredentials() // Ensure credentials are set

	ctx := context.Background()

	// Load default config with MinIO endpoint
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithBaseEndpoint(testMinIOEndpoint))
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	return cfg, nil
}

func getClient() (context.Context, *s3.Client, error) {
	setupMinIOCredentials() // Ensure credentials are set

	cfg, err := getConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get AWS config: %w", err)
	}

	ctx := context.Background()

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// o.BaseEndpoint = aws.String(testMinIOEndpoint)
		o.UsePathStyle = true // MinIO requires path-style requests
	})
	return ctx, client, nil
}

func isMinIOAvailable() bool {
	setupMinIOCredentials() // Ensure credentials are set

	ctx, client, err := getClient()
	if err != nil {
		return false
	}
	_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
	return err == nil
}

func setupTestBucket() error {
	setupMinIOCredentials() // Ensure credentials are set

	ctx, client, err := getClient()
	if err != nil {
		return err
	}
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		return err
	}
	return nil
}

func cleanupTestBucket() {
	setupMinIOCredentials() // Ensure credentials are set

	ctx, client, err := getClient()
	if err != nil {
		fmt.Printf("Failed to get S3 client: %v\n", err)
		return
	}

	// List and delete all objects in bucket
	listResp, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(testBucket),
	})
	for _, obj := range listResp.Contents {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    obj.Key,
		})
		if err != nil {
			fmt.Printf("Failed to delete object %s: %v\n", *obj.Key, err)
		}
	}

	// Delete bucket
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		fmt.Printf("Failed to delete bucket %s: %v\n", testBucket, err)
	}
}

func createTestFiles(t *testing.T) map[string]string {
	tmpDir := t.TempDir()
	files := make(map[string]string)

	// Create small file (< 5MB)
	smallFile := filepath.Join(tmpDir, "small.txt")
	smallContent := strings.Repeat("Hello World!\n", 1000) // ~13KB
	if err := os.WriteFile(smallFile, []byte(smallContent), 0644); err != nil {
		t.Fatalf("Failed to create small test file: %v", err)
	}
	files["small"] = smallFile

	// Create large file (> 5MB for multipart)
	largeFile := filepath.Join(tmpDir, "large.txt")
	largeContent := strings.Repeat("A", 16*1024*1024) // 16MB
	if err := os.WriteFile(largeFile, []byte(largeContent), 0644); err != nil {
		t.Fatalf("Failed to create large test file: %v", err)
	}
	files["large"] = largeFile

	return files
}

func cleanupTestFiles(files map[string]string) {
	for _, filePath := range files {
		os.Remove(filePath)
	}
}

func verifyUpload(fileName string) error {
	setupMinIOCredentials() // Ensure credentials are set

	ctx, client, err := getClient()
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(filepath.Join(testPathPrefix, fileName)),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read at least some content to verify the file exists and is readable
	buf := make([]byte, 1024)
	_, err = io.ReadAtLeast(resp.Body, buf, 1)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("failed to read uploaded file: %w", err)
	}

	return nil
}
