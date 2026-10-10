// Rate-limit backends. The platform injects REDIS_URL when the deploy.yaml
// cache block is declared; without it the in-memory limiter is used, exactly
// as before (fine for a single replica, per-pod otherwise).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// limiter gates signup-style POSTs per client IP.
type limiter interface {
	allow(ip string) bool
}

const redisTimeout = 2 * time.Second

// redisLimiter is a fixed-window counter shared across replicas: INCR on
// rl:<ip>:<unix-minute> with a 90s expiry, same 5/min budget as the
// in-memory limiter. Redis errors inside allow fail OPEN — a cache outage
// must never block signups — and are logged once to avoid spam.
type redisLimiter struct {
	client  *redis.Client
	logOnce sync.Once
}

// newRedisLimiter dials and pings so a bad REDIS_URL is caught at startup,
// where main falls back to the in-memory limiter.
func newRedisLimiter(redisURL string) (*redisLimiter, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}
	return &redisLimiter{client: client}, nil
}

func (rl *redisLimiter) allow(ip string) bool {
	key := fmt.Sprintf("rl:%s:%d", ip, time.Now().Unix()/60)
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
	defer cancel()
	pipe := rl.client.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 90*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		rl.logOnce.Do(func() {
			slog.Error("redis rate limit failed; failing open", "err", err)
		})
		return true
	}
	return incr.Val() <= rateLimit
}
