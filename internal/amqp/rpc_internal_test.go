package amqp

import (
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"
)

func TestExpiredCountsTheSecondOfResolutionOfTheTimestamp(t *testing.T) {
	sent := time.Date(2026, 9, 25, 12, 0, 0, 900_000_000, time.UTC)
	d := rabbitmq.Delivery{Timestamp: sent.Truncate(time.Second), Expiration: "1500"}
	boundary := sent.Truncate(time.Second).Add(time.Second + 1500*time.Millisecond)

	if expired(d, sent.Add(time.Second)) {
		t.Fatal("sent at .900 with 1.5 s must still be alive 1 s later, although the stamp says .000")
	}
	if expired(d, boundary.Add(-time.Millisecond)) {
		t.Fatal("just before stamp + 1 s + expiration the request is alive")
	}
	if !expired(d, boundary) {
		t.Fatal("at stamp + 1 s + expiration the request is expired")
	}
	if got := requestDeadline(d, sent); !got.Equal(boundary) {
		t.Fatalf("the handler deadline counts from the stamp, got %s want %s", got, boundary)
	}
}

func TestExpiredWithoutTimestampCountsFromReceipt(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	d := rabbitmq.Delivery{Expiration: "1500"}
	if expired(d, now) {
		t.Fatal("a request without timestamp is never expired on arrival")
	}
	if got := requestDeadline(d, now); !got.Equal(now.Add(1500 * time.Millisecond)) {
		t.Fatalf("deadline %s", got)
	}
}
