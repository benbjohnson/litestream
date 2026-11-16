package s3

import (
	"context"
	"net"
	"time"
)

const (
	// responseHeaderTimeout bounds how long the transport waits for response
	// headers after a request has been fully written. A provider that accepts a
	// request and never answers would otherwise block the call indefinitely,
	// because the SDK retries only after an attempt fails.
	responseHeaderTimeout = 30 * time.Second

	// writeTimeout bounds a single blocked socket write. The deadline is
	// refreshed on every write, so it limits a stalled connection but never the
	// total duration of an upload that keeps making progress.
	writeTimeout = 30 * time.Second
)

// withWriteTimeout wraps dial so that each Write on the returned connection
// must make progress within writeTimeout. TLS writes each record through this
// connection, so a large upload refreshes the deadline as it proceeds.
func withWriteTimeout(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &writeTimeoutConn{Conn: conn}, nil
	}
}

type writeTimeoutConn struct{ net.Conn }

func (c *writeTimeoutConn) Write(p []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
