package litestream

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	DefaultHeartbeatInterval = 5 * time.Minute
	DefaultHeartbeatTimeout  = 30 * time.Second
	MinHeartbeatInterval     = 1 * time.Minute
)

type HeartbeatClient struct {
	mu         sync.Mutex
	httpClient *http.Client

	URL      string
	Interval time.Duration
	Timeout  time.Duration

	lastPingAt time.Time
}

func NewHeartbeatClient(url string, interval time.Duration) *HeartbeatClient {
	if interval < MinHeartbeatInterval {
		interval = MinHeartbeatInterval
	}

	timeout := DefaultHeartbeatTimeout

	return &HeartbeatClient{
		URL:      url,
		Interval: interval,
		Timeout:  timeout,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *HeartbeatClient) Ping(ctx context.Context) error {
	if c.URL == "" {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *HeartbeatClient) ShouldPing() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.lastPingAt) >= c.Interval
}

func (c *HeartbeatClient) LastPingAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPingAt
}

func (c *HeartbeatClient) RecordPing() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastPingAt = time.Now()
}
