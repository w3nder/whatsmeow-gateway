package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
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
	GroupPrefetch        int
	ShardLockTTL         time.Duration
	SendTimeout          time.Duration
	ShutdownDrainTimeout time.Duration
	RpcDrainTimeout      time.Duration
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
		ShardCount:           ownership.DefaultShardCount,
		SendPacePerSec:       1,
		Prefetch:             32,
		GroupPrefetch:        amqp.DefaultGroupPrefetch,
		ShardLockTTL:         24 * time.Hour,
		SendTimeout:          30 * time.Second,
		ShutdownDrainTimeout: 20 * time.Second,
		RpcDrainTimeout:      30 * time.Second,
		CallTmpDir:           os.Getenv("GATEWAY_CALL_TMPDIR"),
		CallRecord:           os.Getenv("GATEWAY_CALL_RECORD") != "false",
		CallMediaAddr:        os.Getenv("CALL_MEDIA_ADDR"),
		CallMediaTokenSecret: os.Getenv("CALL_MEDIA_TOKEN_SECRET"),
		CallMediaOrigins:     splitCSV(os.Getenv("CALL_MEDIA_ALLOWED_ORIGINS")),
	}
	if c.AMQPURL == "" || c.SessionDSN == "" || c.RedisURL == "" || c.InstanceID == "" {
		return Config{}, fmt.Errorf("missing required env (GATEWAY_INSTANCE_ID, AMQP_URL, SESSION_DATABASE_URL, REDIS_URL)")
	}
	if raw := os.Getenv("GROUP_PREFETCH"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("GROUP_PREFETCH must be a positive integer, got %q", raw)
		}
		c.GroupPrefetch = n
	}
	if c.CallMediaAddr != "" && c.CallMediaTokenSecret == "" {
		return Config{}, fmt.Errorf("CALL_MEDIA_TOKEN_SECRET is required when CALL_MEDIA_ADDR is set")
	}
	return c, nil
}

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
