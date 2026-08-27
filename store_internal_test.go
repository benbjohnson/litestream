package litestream

import (
	"testing"
	"time"
)

func TestBusyRetryDelay(t *testing.T) {
	for _, tt := range []struct {
		name      string
		nextDelay time.Duration
		streak    int
		maxRetry  time.Duration
		want      time.Duration
	}{
		{"not busy keeps interval", time.Hour, 0, 10 * time.Second, time.Hour},
		{"first busy pass retries after base", time.Hour, 1, 10 * time.Second, time.Second},
		{"second busy pass doubles", time.Hour, 2, 10 * time.Second, 2 * time.Second},
		{"fourth busy pass doubles again", time.Hour, 4, 10 * time.Second, 8 * time.Second},
		{"backoff is capped", time.Hour, 20, 10 * time.Second, 10 * time.Second},
		{"busy keeps shorter interval", 500 * time.Millisecond, 3, 10 * time.Second, 500 * time.Millisecond},
		{"busy with retry disabled", time.Hour, 1, 0, time.Hour},
		{"negative clamps to zero", -time.Second, 0, 10 * time.Second, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := busyRetryDelay(tt.nextDelay, tt.streak, tt.maxRetry); got != tt.want {
				t.Fatalf("busyRetryDelay()=%s, want %s", got, tt.want)
			}
		})
	}
}
