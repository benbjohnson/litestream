package s3

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
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
	conn := &writeTimeoutConn{Conn: underlying}
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
	underlying := &deadlineTestConn{deadlineErr: want}
	conn := &writeTimeoutConn{Conn: underlying}
	if n, err := conn.Write([]byte("part")); n != 0 || !errors.Is(err, want) || underlying.writes != 0 {
		t.Fatal("deadline failure must refuse the write")
	}
}

func TestS3WriteTimeoutBlockedAndIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, peer := net.Pipe()
		defer local.Close()
		defer peer.Close()
		conn := &writeTimeoutConn{Conn: local}
		for range 2 {
			time.Sleep(writeTimeout + time.Second)
			done := make(chan error, 1)
			go func() {
				var b [1]byte
				_, err := io.ReadFull(peer, b[:])
				done <- err
			}()
			if _, err := conn.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		guard := time.AfterFunc(2*writeTimeout, func() { peer.Close() })
		defer guard.Stop()
		start := time.Now()
		n, err := conn.Write([]byte("blocked"))
		var timeout net.Error
		if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() || time.Since(start) != writeTimeout {
			t.Fatalf("blocked write = %d, %v after %s", n, err, time.Since(start))
		}
		if !newTransportRetryer().IsErrorRetryable(err) {
			t.Fatalf("timeout is not retryable: %v", err)
		}
	})
}

func TestS3TransportResponseHeaderTimeout(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		name := "HTTP1"
		if http2 {
			name = "HTTP2"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			started := make(chan struct{})
			cancelStarted := make(chan struct{})
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/stall" {
					close(started)
					<-r.Context().Done()
					return
				}
				if r.URL.Path == "/cancel" {
					close(cancelStarted)
					<-r.Context().Done()
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			server.EnableHTTP2 = http2
			server.StartTLS()
			defer server.Close()
			client := NewReplicaClient()
			client.Bucket, client.Region, client.Endpoint = "test", "us-east-1", server.URL
			client.AccessKeyID, client.SecretAccessKey = "test", "test"
			client.SkipVerify = true
			if err := client.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			transport := client.s3.Options().HTTPClient.(*http.Client).Transport.(*http.Transport)
			defer transport.CloseIdleConnections()
			dialCtx, cancelDial := context.WithCancel(context.Background())
			cancelDial()
			if conn, err := transport.DialContext(dialCtx, "tcp", server.Listener.Addr().String()); !errors.Is(err, context.Canceled) || conn != nil {
				if conn != nil {
					conn.Close()
				}
				t.Fatalf("canceled dial = %v, %v", conn, err)
			}
			conn, err := transport.DialContext(context.Background(), "tcp", server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			_, wrapped := conn.(*writeTimeoutConn)
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if !wrapped {
				t.Fatal("transport connection does not bound writes")
			}

			ctx, cancel := context.WithTimeout(context.Background(), responseHeaderTimeout+10*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/stall", nil)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			done := make(chan error, 1)
			go func() {
				resp, err := transport.RoundTrip(req)
				if resp != nil {
					err = errors.Join(err, resp.Body.Close())
				}
				done <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			healthy, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/healthy", nil)
			if err != nil {
				t.Fatal(err)
			}
			for range 4 {
				resp, err := transport.RoundTrip(healthy)
				if err != nil {
					t.Fatal(err)
				}
				if err := resp.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			cancelCtx, cancelRequest := context.WithCancel(ctx)
			defer cancelRequest()
			cancelReq, err := http.NewRequestWithContext(cancelCtx, http.MethodGet, server.URL+"/cancel", nil)
			if err != nil {
				t.Fatal(err)
			}
			canceled := make(chan error, 1)
			go func() {
				resp, err := transport.RoundTrip(cancelReq)
				if resp != nil {
					err = errors.Join(err, resp.Body.Close())
				}
				canceled <- err
			}()
			select {
			case <-cancelStarted:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			cancelRequest()
			if err := <-canceled; !errors.Is(err, context.Canceled) {
				t.Fatalf("in-flight cancellation = %v", err)
			}
			err = <-done
			var timeout net.Error
			if !errors.As(err, &timeout) || !timeout.Timeout() || ctx.Err() != nil || time.Since(start) < responseHeaderTimeout {
				t.Fatalf("header wait = %v, context = %v, elapsed = %s", err, ctx.Err(), time.Since(start))
			}
			if !newTransportRetryer().IsErrorRetryable(err) {
				t.Fatalf("timeout is not retryable: %v", err)
			}
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/healthy", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			wantProto := 1
			if http2 {
				wantProto = 2
			}
			if resp.ProtoMajor != wantProto {
				t.Fatalf("protocol = %d, want %d", resp.ProtoMajor, wantProto)
			}
			cancel()
			if resp, err := transport.RoundTrip(req); !errors.Is(err, context.Canceled) {
				if resp != nil {
					resp.Body.Close()
				}
				t.Fatalf("canceled request = %v", err)
			}
		})
	}
}
