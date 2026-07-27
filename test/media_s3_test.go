package test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/w3nder/whatsmeow-gateway/internal/media"
)

const minioImage = "minio/minio:RELEASE.2024-01-16T16-07-38Z"

func TestS3StorePutRoundTrip(t *testing.T) {
	ctx := context.Background()

	container, err := minio.Run(ctx, minioImage)
	if err != nil {
		t.Fatalf("failed to start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate minio container: %v", err)
		}
	})

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	const bucket = "vectax-media-test"
	rawS3, err := rawS3Client(ctx, endpoint, container.Username, container.Password)
	if err != nil {
		t.Fatalf("failed to build raw s3 client: %v", err)
	}
	if _, err := rawS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	store, err := media.NewS3Store(ctx, media.S3Config{
		Bucket:          bucket,
		Region:          "us-east-1",
		Endpoint:        "http://" + endpoint,
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	key := "inbound-media/tenant-1/wamid.stub"
	data := []byte("hello media bytes")

	if err := store.Put(ctx, key, "image/jpeg", data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer func() {
		if err := obj.Body.Close(); err != nil {
			t.Errorf("failed to close object body: %v", err)
		}
	}()

	got, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("failed to read object body: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("expected body %q, got %q", data, got)
	}
	if aws.ToString(obj.ContentType) != "image/jpeg" {
		t.Fatalf("expected ContentType=image/jpeg, got %q", aws.ToString(obj.ContentType))
	}
}

func TestS3StorePutMissingBucketFails(t *testing.T) {
	ctx := context.Background()

	container, err := minio.Run(ctx, minioImage)
	if err != nil {
		t.Fatalf("failed to start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate minio container: %v", err)
		}
	})

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	store, err := media.NewS3Store(ctx, media.S3Config{
		Bucket:          "does-not-exist",
		Region:          "us-east-1",
		Endpoint:        "http://" + endpoint,
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	if err := store.Put(ctx, "inbound-media/tenant-1/wamid.stub", "image/jpeg", []byte("data")); err == nil {
		t.Fatal("expected error when bucket does not exist")
	}
}

func rawS3Client(ctx context.Context, endpoint, accessKey, secretKey string) (*s3.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	}), nil
}
