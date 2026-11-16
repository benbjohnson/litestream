package s3

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type deadlineTestConn struct {
	net.Conn
	deadlines   []time.Time
	writes      int
	deadlineErr error
}

func (c *deadlineTestConn) SetWriteDeadline(deadline time.Time) error {
	c.deadlines = append(c.deadlines, deadline)
	return c.deadlineErr
}

func (c *deadlineTestConn) Write(p []byte) (int, error) {
	c.writes++
	return len(p) - 1, io.ErrShortWrite
}

func TestS3WriteTimeoutRefreshesEachWrite(t *testing.T) {
	underlying := &deadlineTestConn{}
	dial := withWriteTimeout(func(context.Context, string, string) (net.Conn, error) {
		return underlying, nil
	})
	conn, err := dial(context.Background(), "tcp", "example.test:443")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		before := time.Now()
		n, err := conn.Write([]byte("part"))
		after := time.Now()
		if n != 3 || !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("write result = %d, %v", n, err)
		}
		if len(underlying.deadlines) != i+1 || underlying.writes != i+1 {
			t.Fatal("every write must receive a fresh deadline")
		}
		deadline := underlying.deadlines[i]
		if deadline.Before(before.Add(writeTimeout)) || deadline.After(after.Add(writeTimeout)) {
			t.Fatal("deadline must bound this write rather than the whole upload")
		}
	}
}

func TestS3WriteTimeoutPropagatesSetupErrors(t *testing.T) {
	want := errors.New("connection unavailable")
	dial := withWriteTimeout(func(context.Context, string, string) (net.Conn, error) {
		return nil, want
	})
	if conn, err := dial(context.Background(), "tcp", "example.test:443"); conn != nil || !errors.Is(err, want) {
		t.Fatal("dial failure must propagate")
	}
	underlying := &deadlineTestConn{deadlineErr: want}
	conn := &writeTimeoutConn{Conn: underlying}
	if n, err := conn.Write([]byte("part")); n != 0 || !errors.Is(err, want) || underlying.writes != 0 {
		t.Fatal("deadline failure must refuse the write")
	}
}
