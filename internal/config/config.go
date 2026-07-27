package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	InstanceID        string
	AMQPURL           string
	SessionDSN        string
	RedisURL          string
	S3Bucket          string
	S3Region          string
	S3Endpoint        string
	S3AccessKeyID     string
	S3SecretAccessKey string
	ShardCount        int
	SendPacePerSec    float64
	Prefetch          int
	ShardLockTTL      time.Duration
}

func Load() (Config, error) {
	c := Config{
		InstanceID:        os.Getenv("GATEWAY_INSTANCE_ID"),
		AMQPURL:           os.Getenv("AMQP_URL"),
		SessionDSN:        os.Getenv("SESSION_DATABASE_URL"),
		RedisURL:          os.Getenv("REDIS_URL"),
		S3Bucket:          os.Getenv("S3_BUCKET"),
		S3Region:          os.Getenv("S3_REGION"),
		S3Endpoint:        os.Getenv("S3_ENDPOINT"),
		S3AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
		S3SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
		ShardCount:        1024,
		SendPacePerSec:    1,
		Prefetch:          32,
		ShardLockTTL:      24 * time.Hour,
	}
	if c.AMQPURL == "" || c.SessionDSN == "" || c.RedisURL == "" || c.InstanceID == "" {
		return Config{}, fmt.Errorf("missing required env (GATEWAY_INSTANCE_ID, AMQP_URL, SESSION_DATABASE_URL, REDIS_URL)")
	}
	return c, nil
}
