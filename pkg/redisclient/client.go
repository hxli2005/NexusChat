package redisclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	inner *redis.Client
}

func New(addr string) *Client {
	if addr == "" {
		addr = "localhost:6379"
	}
	return &Client{inner: redis.NewClient(&redis.Options{Addr: addr})}
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.inner.Ping(ctx).Result()
	return err
}

func (c *Client) SetOnline(ctx context.Context, userID int64, node string, ttl time.Duration) error {
	return c.inner.Set(ctx, onlineKey(userID), node, ttl).Err()
}

func (c *Client) DelOnline(ctx context.Context, userID int64) error {
	return c.inner.Del(ctx, onlineKey(userID)).Err()
}

func (c *Client) GetOnline(ctx context.Context, userID int64) (string, error) {
	val, err := c.inner.Get(ctx, onlineKey(userID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return val, err
}

func (c *Client) CacheRecentMessage(ctx context.Context, conversationID int64, payload []byte, keep int64) error {
	key := messagesKey(conversationID)
	if err := c.inner.LPush(ctx, key, payload).Err(); err != nil {
		return err
	}
	if keep <= 0 {
		keep = 100
	}
	return c.inner.LTrim(ctx, key, 0, keep-1).Err()
}

func (c *Client) GetRecentMessages(ctx context.Context, conversationID int64, limit int64) ([]string, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return c.inner.LRange(ctx, messagesKey(conversationID), 0, limit-1).Result()
}

func (c *Client) Raw() *redis.Client {
	return c.inner
}

func onlineKey(userID int64) string {
	return "online:" + strconvInt64(userID)
}

func messagesKey(conversationID int64) string {
	return "chat:msgs:" + strconvInt64(conversationID)
}

func strconvInt64(v int64) string {
	return fmt.Sprintf("%d", v)
}
