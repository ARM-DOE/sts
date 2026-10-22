package export

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const DefaultPartSize = 5 * 1024 * 1024 // 5 MiB - minimum part size
const DefaultConcurrency = 2            // Default concurrency for multipart uploads

type S3Upload struct {
	bucket     string
	pathPrefix string
	ctx        context.Context
	uploader   *transfermanager.Client
}

func NewS3Upload(cfg aws.Config, bucket, pathPrefix string) (*S3Upload, error) {
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if cfg.BaseEndpoint != nil && *cfg.BaseEndpoint != "" {
			o.UsePathStyle = true
		}
	})
	return &S3Upload{
		bucket:     bucket,
		pathPrefix: pathPrefix,
		ctx:        context.Background(),
		uploader: transfermanager.New(client, func(u *transfermanager.Options) {
			u.PartSizeBytes = DefaultPartSize
			u.MultipartUploadThreshold = DefaultPartSize + 1
			u.Concurrency = DefaultConcurrency
		}),
	}, nil
}

func (s *S3Upload) Upload(filePath, fileName string) error {
	// Upload the file using multipart uploader
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	out, err := s.uploader.UploadObject(s.ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath.Join(s.pathPrefix, fileName)),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	localETag, err := computeS3MultipartETag(filePath, DefaultPartSize)
	if err != nil {
		return fmt.Errorf("failed to compute local ETag: %w", err)
	}

	if localETag != *out.ETag {
		return fmt.Errorf("ETag mismatch: local %s, S3 %s", localETag, *out.ETag)
	}
	return nil
}

func computeS3MultipartETag(filePath string, partSize int64) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	var md5sums [][]byte
	buf := make([]byte, partSize)

	for {
		n, err := io.ReadFull(file, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n > 0 {
				sum := md5.Sum(buf[:n])
				md5sums = append(md5sums, sum[:])
			}
			break
		}
		if err != nil {
			return "", err
		}
		sum := md5.Sum(buf[:n])
		md5sums = append(md5sums, sum[:])
	}

	if len(md5sums) == 1 {
		return `"` + hex.EncodeToString(md5sums[0]) + `"`, nil
	}

	combined := bytes.Join(md5sums, []byte{})
	finalSum := md5.Sum(combined)
	return fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(finalSum[:]), len(md5sums)), nil
}
