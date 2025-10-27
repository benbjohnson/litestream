package litestream

import (
	"context"
	"time"
)

type contextKey int

const (
	ltxTimestampKey contextKey = iota
)

func WithLTXTimestamp(ctx context.Context, timestamp time.Time) context.Context {
	return context.WithValue(ctx, ltxTimestampKey, timestamp)
}

func LTXTimestampFromContext(ctx context.Context) (time.Time, bool) {
	val := ctx.Value(ltxTimestampKey)
	if val == nil {
		return time.Time{}, false
	}
	timestamp, ok := val.(time.Time)
	return timestamp, ok
}
