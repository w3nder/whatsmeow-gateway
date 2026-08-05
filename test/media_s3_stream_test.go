package test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/w3nder/whatsmeow-gateway/internal/media"
)

// TestS3StorePutStreamRoundTrip covers the path call recordings take. The body
// is deliberately past the uploader's single-part threshold, so this exercises
// the multipart upload that Put's []byte path never reaches.
func TestS3StorePutStreamRoundTrip(t *testing.T) {
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

	const bucket = "vectax-calls-test"
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

	data := bytes.Repeat([]byte("meowcaller-recording"), 6*1024*1024/20)
	key := "calls/channel-1/CALL1.wav"

	if err := store.PutStream(ctx, key, "audio/wav", bytes.NewReader(data)); err != nil {
		t.Fatalf("PutStream failed: %v", err)
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
		t.Fatalf("body length %d, want %d", len(got), len(data))
	}
	if aws.ToString(obj.ContentType) != "audio/wav" {
		t.Fatalf("ContentType = %q, want audio/wav", aws.ToString(obj.ContentType))
	}
}
