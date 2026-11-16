package s3

import (
	"net"
	"time"
)

const (
	// responseHeaderTimeout bounds how long the transport waits for response
	// headers after a request has been fully written. A provider that accepts a
	// request and never answers would otherwise block the call indefinitely,
	// because the SDK retries only after an attempt fails.
	responseHeaderTimeout = 30 * time.Second

	writeTimeout = 30 * time.Second
)

type writeTimeoutConn struct{ net.Conn }

func (c *writeTimeoutConn) Write(p []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
