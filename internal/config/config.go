package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	InstanceID           string
	AMQPURL              string
	SessionDSN           string
	RedisURL             string
	S3Bucket             string
	S3Region             string
	S3Endpoint           string
	S3AccessKeyID        string
	S3SecretAccessKey    string
	ShardCount           int
	SendPacePerSec       float64
	Prefetch             int
	ShardLockTTL         time.Duration
	SendTimeout          time.Duration
	ShutdownDrainTimeout time.Duration
	CallTmpDir           string
	CallRecord           bool
	CallMediaAddr        string
	CallMediaTokenSecret string
	CallMediaOrigins     []string
}

func Load() (Config, error) {
	c := Config{
		InstanceID:           os.Getenv("GATEWAY_INSTANCE_ID"),
		AMQPURL:              os.Getenv("AMQP_URL"),
		SessionDSN:           os.Getenv("SESSION_DATABASE_URL"),
		RedisURL:             os.Getenv("REDIS_URL"),
		S3Bucket:             os.Getenv("S3_BUCKET"),
		S3Region:             os.Getenv("S3_REGION"),
		S3Endpoint:           os.Getenv("S3_ENDPOINT"),
		S3AccessKeyID:        os.Getenv("S3_ACCESS_KEY_ID"),
		S3SecretAccessKey:    os.Getenv("S3_SECRET_ACCESS_KEY"),
		ShardCount:           1024,
		SendPacePerSec:       1,
		Prefetch:             32,
		ShardLockTTL:         24 * time.Hour,
		SendTimeout:          30 * time.Second,
		ShutdownDrainTimeout: 20 * time.Second,
		CallTmpDir:           os.Getenv("GATEWAY_CALL_TMPDIR"),
		// Recording is on by default: a call the backend cannot hear afterwards
		// is of little use to it. GATEWAY_CALL_RECORD=false turns it off.
		CallRecord: os.Getenv("GATEWAY_CALL_RECORD") != "false",
		// Empty leaves the media websocket off, which is today's behaviour: an
		// instance that never sets CALL_MEDIA_ADDR runs exactly as it did before
		// this listener existed. Operators who want it set it to, e.g., :8081.
		CallMediaAddr:        os.Getenv("CALL_MEDIA_ADDR"),
		CallMediaTokenSecret: os.Getenv("CALL_MEDIA_TOKEN_SECRET"),
		// The operator connects from the frontend's origin, not this
		// listener's, so without an explicit allowlist the websocket library's
		// default same-origin check refuses every real browser connection.
		// Comma separated host patterns, e.g. "app.example.com".
		CallMediaOrigins: splitCSV(os.Getenv("CALL_MEDIA_ALLOWED_ORIGINS")),
	}
	if c.AMQPURL == "" || c.SessionDSN == "" || c.RedisURL == "" || c.InstanceID == "" {
		return Config{}, fmt.Errorf("missing required env (GATEWAY_INSTANCE_ID, AMQP_URL, SESSION_DATABASE_URL, REDIS_URL)")
	}
	// The secret authenticates every media-socket request; a listener with no
	// secret would accept any token, signed by anyone, for any call.
	if c.CallMediaAddr != "" && c.CallMediaTokenSecret == "" {
		return Config{}, fmt.Errorf("CALL_MEDIA_TOKEN_SECRET is required when CALL_MEDIA_ADDR is set")
	}
	return c, nil
}

// splitCSV parses a comma separated env var, trimming whitespace and
// dropping empty entries -- "" and " " both mean "nothing configured" rather
// than a single blank pattern.
func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
