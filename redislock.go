package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// RedisLockOptions type
type RedisLockOptions struct {
	// Default: 0 = do not retry
	RetryCount int32

	// Default: 100ms
	RetryDelay time.Duration

	// Default: 300s
	KeyExpiration time.Duration
}

func (opt *RedisLockOptions) init() {
	if opt.RetryCount == 0 {
		opt.RetryCount = 0
	}

	if opt.RetryDelay == 0 {
		opt.RetryDelay = 100 * time.Millisecond
	}

	if opt.KeyExpiration == 0 {
		opt.KeyExpiration = 300 * time.Second
	}
}

// RedisLockClient interface
type RedisLockClient interface {
	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(keys ...string) *redis.IntCmd
}

// RedisLocker interface
type RedisLocker interface {
	NewLock(key string, opts *RedisLockOptions) (Locker, error)
}

// NewRedisLock 創建出一個RedisLock
func NewRedisLock(client RedisLockClient) RedisLocker {
	return &RedisLock{
		client: client,
	}
}

// RedisLock type
type RedisLock struct {
	client RedisLockClient
}

// NewLock 建立新的Lock
func (rl *RedisLock) NewLock(key string, opts *RedisLockOptions) (Locker, error) {
	opts.init()

	attempts := opts.RetryCount + 1
	retryDelay := time.NewTimer(opts.RetryDelay)

	defer retryDelay.Stop()

	for {
		ret, err := rl.client.SetNX(key, 1, opts.KeyExpiration).Result()

		if err != nil {
			if attempts--; attempts <= 0 {
				return nil, fmt.Errorf("NewLock err %w", err)
			}

			goto retry
		}

		if ret {
			return &Lock{
				client: rl.client,
				key:    key,
			}, nil
		}

		if attempts--; attempts <= 0 {
			return nil, fmt.Errorf("NewLock redis lock %s key exist", key)
		}

	retry:
		<-retryDelay.C

		retryDelay.Reset(opts.RetryDelay)
	}
}

// Locker interface
type Locker interface {
	Lock() error
	Unlock() error
}

// Lock type (多線程不安全)
type Lock struct {
	client RedisLockClient
	key    string
}

// Lock 再次上鎖
func (l *Lock) Lock() error {
	ret, err := l.client.SetNX(l.key, 1, 0).Result()

	if err != nil {
		return fmt.Errorf("Lock %s key %w", l.key, err)
	}

	if !ret {
		err = fmt.Errorf("Lock redis lock %s key exist", l.key)
	}

	return err
}

// Unlock 解鎖
func (l *Lock) Unlock() error {
	_, err := l.client.Del(l.key).Result()

	if err != nil {
		return fmt.Errorf("Unlock %s key %w", l.key, err)
	}

	return err
}
