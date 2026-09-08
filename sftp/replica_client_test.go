package sftp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
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

func TestReplicaClient_CloseInterruptsRequest(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{NoClientAuth: true}
	config.AddHostKey(signer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverErr <- err
			return
		}
		_, channels, requests, err := ssh.NewServerConn(conn, config)
		if err != nil {
			serverErr <- err
			return
		}
		go ssh.DiscardRequests(requests)
		for ch := range channels {
			channel, requests, err := ch.Accept()
			if err != nil {
				serverErr <- err
				return
			}
			for request := range requests {
				if request.Type != "subsystem" {
					if err := request.Reply(false, nil); err != nil {
						serverErr <- err
						return
					}
					continue
				}
				if err := request.Reply(true, nil); err != nil {
					serverErr <- err
					return
				}
				server := sftp.NewRequestServer(channel, sftp.Handlers{FileList: &blockedFileLister{started: started, release: release}})
				serverErr <- errors.Join(server.Serve(), server.Close())
				return
			}
		}
	}()

	client := NewReplicaClient()
	client.Host = listener.Addr().String()
	client.User = "test"
	client.HostKey = string(ssh.MarshalAuthorizedKey(signer.PublicKey()))
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := client.Init(ctx); err != nil {
		t.Fatal(err)
	}
	canceledCtx, cancelCause := context.WithCancelCause(ctx)
	cause := errors.New("request canceled")
	cancelCause(cause)
	if err := client.Init(canceledCtx); !errors.Is(err, cause) {
		t.Fatalf("cached Init error = %v, want %v", err, cause)
	}
	requestErr := make(chan error, 1)
	go func() {
		_, err := client.LTXFiles(ctx, 0, 0, false)
		requestErr <- err
	}()
	select {
	case <-started:
	case err := <-serverErr:
		t.Fatalf("server: %v", err)
	case <-ctx.Done():
		t.Fatal("directory request did not reach server")
	}
	closeErr := make(chan error, 1)
	go func() { closeErr <- client.Close() }()
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close blocked on stalled directory request")
	}
	select {
	case err := <-requestErr:
		if err == nil {
			t.Fatal("stalled directory request succeeded")
		}
	case <-ctx.Done():
		t.Fatal("directory request did not stop after Close")
	}
}

type blockedFileLister struct {
	started chan struct{}
	release chan struct{}
}

func (l *blockedFileLister) Filelist(*sftp.Request) (sftp.ListerAt, error) {
	close(l.started)
	<-l.release
	return nil, errors.New("request released")
}
