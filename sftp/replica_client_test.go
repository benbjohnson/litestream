package sftp

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestReplicaClient_InitCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	client := NewReplicaClient()
	client.Host = listener.Addr().String()
	client.User = "test"
	client.DialTimeout = time.Minute
	ctx, cancel := context.WithCancelCause(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- client.Init(ctx) }()

	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
	case err := <-acceptErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("SFTP client did not connect")
	}

	cancelErr := errors.New("request canceled")
	cancel(cancelErr)
	select {
	case err := <-errCh:
		if !errors.Is(err, cancelErr) {
			t.Fatalf("error=%v, want %v", err, cancelErr)
		}
	case <-time.After(time.Second):
		t.Fatal("SFTP initialization did not stop after cancellation")
	}
}
