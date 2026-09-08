package sftp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/superfly/ltx"
	"golang.org/x/crypto/ssh"
)

const (
	fsyncExt = "fsync@openssh.com"

	// SSH_FXP_VERSION reply (with its length prefix) for protocol version 3
	// that advertises the fsync extension.
	versionReply = "\x00\x00\x00\x1f\x02\x00\x00\x00\x03\x00\x00\x00\x11" + fsyncExt + "\x00\x00\x00\x011"
)

func TestReplicaClient_SSHAddr(t *testing.T) {
	for _, tt := range []struct {
		host string
		want string
	}{
		{host: "example.com", want: "example.com:22"},
		{host: "example.com:2222", want: "example.com:2222"},
		{host: "127.0.0.1", want: "127.0.0.1:22"},
		{host: "127.0.0.1:2222", want: "127.0.0.1:2222"},
		{host: "::1", want: "[::1]:22"},
		{host: "[::1]", want: "[::1]:22"},
		{host: "[::1]:2222", want: "[::1]:2222"},
		{host: "[fe80::1%eth0]", want: "[fe80::1%eth0]:22"},
		{host: "[fe80::1%eth0]:2222", want: "[fe80::1%eth0]:2222"},
	} {
		t.Run(tt.host, func(t *testing.T) {
			c := NewReplicaClient()
			c.Host = tt.host
			if got := c.sshAddr(); got != tt.want {
				t.Fatalf("sshAddr()=%q, want %q", got, tt.want)
			}
		})
	}
}

func TestReplicaClient_WriteLTXFile_Fsync(t *testing.T) {
	hdr := ltx.Header{Version: ltx.Version, PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: time.Now().UnixMilli()}
	data, _ := hdr.MarshalBinary()

	t.Run("BeforeRename", func(t *testing.T) {
		c, requests := newFsyncClient(t, 0)
		if _, err := c.WriteLTXFile(context.Background(), 1, 1, 1, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if got, want := requests(), []string{"open", "fsync", "rename"}; !slices.Equal(got, want) {
			t.Fatalf("requests=%v, want %v", got, want)
		}
	})

	t.Run("SyncError", func(t *testing.T) {
		c, _ := newFsyncClient(t, 4) // SSH_FX_FAILURE
		_, err := c.WriteLTXFile(context.Background(), 1, 1, 1, bytes.NewReader(data))
		if err == nil || !strings.Contains(err.Error(), "cannot sync temporary ltx file") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// newFsyncClient returns a client for an in-process pkg/sftp server, which
// does not implement fsync@openssh.com. A proxy in between advertises the
// extension, answers fsync requests with the given status code, and records
// open, fsync and rename requests for the returned function.
func newFsyncClient(t *testing.T, status byte) (*ReplicaClient, func() []string) {
	t.Helper()

	clientConn, clientProxy := net.Pipe()
	serverProxy, serverConn := net.Pipe()
	server, err := sftp.NewServer(serverConn)
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve()
	go io.Copy(clientProxy, serverProxy)

	// Forward client requests to the server. The client waits for each reply, so
	// the proxy's replies never interleave with the server's, and the recorded
	// requests are ordered before the call returns without a lock.
	var reqs []string
	go func() {
		defer serverProxy.Close()
		for {
			pkt, err := readPacket(clientProxy) // length, type, request id, body
			if err != nil {
				return
			}

			var reply []byte
			switch pkt[4] {
			case 1: // SSH_FXP_INIT
				reply = []byte(versionReply)
			case 3: // SSH_FXP_OPEN
				reqs = append(reqs, "open")
			case 18: // SSH_FXP_RENAME
				reqs = append(reqs, "rename")
			case 200: // SSH_FXP_EXTENDED
				if bytes.Contains(pkt, []byte(fsyncExt)) {
					reqs = append(reqs, "fsync")
					// SSH_FXP_STATUS with the request id, code, empty message and language tag.
					reply = append(append([]byte{0, 0, 0, 17, 101}, pkt[5:9]...), 0, 0, 0, status, 0, 0, 0, 0, 0, 0, 0, 0)
				}
			}

			if reply != nil {
				_, err = clientProxy.Write(reply)
			} else {
				_, err = serverProxy.Write(pkt)
			}
			if err != nil {
				return
			}
		}
	}()

	sftpClient, err := sftp.NewClientPipe(clientConn, clientConn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sftpClient.Close() })

	c := NewReplicaClient()
	c.Path = t.TempDir()
	c.sftpClient = sftpClient
	return c, func() []string { return reqs }
}

// readPacket reads one SFTP packet, including its length prefix.
func readPacket(r io.Reader) ([]byte, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	pkt := append(n[:], make([]byte, binary.BigEndian.Uint32(n[:]))...)
	_, err := io.ReadFull(r, pkt[4:])
	return pkt, err
}

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
