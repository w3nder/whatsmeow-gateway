package media

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Config struct {
	Bucket          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}

type S3Store struct {
	client *s3.Client
	bucket string
}

func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		// The SDK default (WhenSupported) sends PutObject bodies with
		// `Content-Encoding: aws-chunked` and a trailing CRC32 checksum, which
		// S3-compatible providers (e.g. Magalu Cloud) reject with
		// `501 NotImplemented: AWS chunked encoding not supported`.
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		return nil, fmt.Errorf("media: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})

	return &S3Store{client: client, bucket: cfg.Bucket}, nil
}

// PutStream uploads r under key without holding it in memory. Call recordings
// run to hundreds of megabytes for a long call, so Put's []byte would mean
// buffering the whole thing in RAM per concurrent call.
//
// r must be seekable -- both callers pass an *os.File -- because the SDK sizes
// the body by seeking it. A non-seekable reader is rejected here rather than
// left to fail deep inside the SDK with an unrelated-looking error.
func (s *S3Store) PutStream(ctx context.Context, key, mime string, r io.Reader) error {
	if _, ok := r.(io.ReadSeeker); !ok {
		return fmt.Errorf("media: put stream %s: body is not seekable", key)
	}

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String(mime),
	})
	if err != nil {
		return fmt.Errorf("media: put stream %s: %w", key, err)
	}
	return nil
}

func (s *S3Store) Put(ctx context.Context, key, mime string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(mime),
	})
	if err != nil {
		return fmt.Errorf("media: put object %s: %w", key, err)
	}
	return nil
}
