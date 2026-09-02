package test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/media"
)

func TestS3StorePutWithoutAWSChunkedEncoding(t *testing.T) {
	ctx := context.Background()

	var (
		gotBody      []byte
		gotEncoding  string
		gotTrailer   string
		gotLength    string
		gotChecksum  string
		gotAlgorithm string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEncoding = r.Header.Get("Content-Encoding")
		gotTrailer = r.Header.Get("X-Amz-Trailer")
		gotLength = r.Header.Get("Content-Length")
		gotChecksum = r.Header.Get("X-Amz-Checksum-Crc32")
		gotAlgorithm = r.Header.Get("X-Amz-Sdk-Checksum-Algorithm")

		if strings.Contains(gotEncoding, "aws-chunked") || gotTrailer != "" {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
				`<Error><Code>NotImplemented</Code>`+
				`<Message>AWS chunked encoding not supported.</Message></Error>`)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		gotBody = body
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	store, err := media.NewS3Store(ctx, media.S3Config{
		Bucket:          "vectax-media-test",
		Region:          "us-east-1",
		Endpoint:        srv.URL,
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	data := []byte("hello media bytes")
	if err := store.Put(ctx, "inbound-media/tenant-1/wamid.stub", "image/jpeg", data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if strings.Contains(gotEncoding, "aws-chunked") {
		t.Fatalf("expected no aws-chunked Content-Encoding, got %q", gotEncoding)
	}
	if gotTrailer != "" {
		t.Fatalf("expected no x-amz-trailer header, got %q", gotTrailer)
	}
	if gotAlgorithm != "" || gotChecksum != "" {
		t.Fatalf("expected no SDK-computed checksum headers, got algorithm=%q crc32=%q", gotAlgorithm, gotChecksum)
	}
	if gotLength != "17" {
		t.Fatalf("expected Content-Length=17 (raw payload), got %q", gotLength)
	}
	if string(gotBody) != string(data) {
		t.Fatalf("expected body %q, got %q", data, gotBody)
	}
}
