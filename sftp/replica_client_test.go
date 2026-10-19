package sftp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/superfly/ltx"
)

const (
	fsyncExt = "fsync@openssh.com"

	// SSH_FXP_VERSION reply (with its length prefix) for protocol version 3
	// that advertises the fsync extension.
	versionReply = "\x00\x00\x00\x1f\x02\x00\x00\x00\x03\x00\x00\x00\x11" + fsyncExt + "\x00\x00\x00\x011"
)

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
