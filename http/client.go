package http

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/benbjohnson/litestream"
)

// Client represents an client for a streaming Litestream HTTP server.
type Client struct {
	// Upstream endpoint
	URL string

	// Path of database on upstream server.
	Path string

	// Underlying HTTP client
	HTTPClient *http.Client
}

// NewClient returns an instance of Client.
func NewClient(rawurl, path string) *Client {
	return &Client{
		URL:        rawurl,
		Path:       path,
		HTTPClient: http.DefaultClient,
	}
}

// Stream returns a snapshot and continuous stream of WAL updates.
func (c *Client) Stream(ctx context.Context, pos litestream.Pos) (litestream.StreamReader, error) {
	u, err := url.Parse(c.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid client URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return nil, fmt.Errorf("URL host required")
	}

	// Add path & position to query path.
	q := url.Values{"path": []string{c.Path}}
	if !pos.IsZero() {
		q.Set("generation", pos.Generation)
		q.Set("index", litestream.FormatIndex(pos.Index))
		q.Set("offset", litestream.FormatOffset(pos.Offset))
	}

	// Strip off everything but the scheme & host.
	*u = url.URL{
		Scheme:   u.Scheme,
		Host:     u.Host,
		Path:     "/stream",
		RawQuery: q.Encode(),
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}

	pageSize, _ := strconv.Atoi(resp.Header.Get("Litestream-page-size"))
	if pageSize <= 0 {
		resp.Body.Close()
		return nil, fmt.Errorf("stream page size unavailable")
	}

	return &StreamReader{
		pageSize: pageSize,
		rc:       resp.Body,
		lr:       io.LimitedReader{R: resp.Body},
	}, nil
}

// StreamReader represents an optional snapshot followed by a continuous stream
// of WAL updates. It is used to implement live read replication from a single
// primary Litestream server to one or more remote Litestream replicas.
type StreamReader struct {
	pageSize int
	rc       io.ReadCloser
	lr       io.LimitedReader
}

// Close closes the underlying reader.
func (r *StreamReader) Close() (err error) {
	if e := r.rc.Close(); err == nil {
		err = e
	}
	return err
}

// PageSize returns the page size on the remote database.
func (r *StreamReader) PageSize() int { return r.pageSize }

// Read reads bytes of the current payload into p. Only valid after a successful
// call to Next(). On io.EOF, call Next() again to begin reading next record.
func (r *StreamReader) Read(p []byte) (n int, err error) {
	return r.lr.Read(p)
}

// Next returns the next available record. This call will block until a record
// is available. After calling Next(), read the payload from the reader using
// Read() until io.EOF is reached.
func (r *StreamReader) Next() (*litestream.StreamRecordHeader, error) {
	// If bytes remain on the current file, discard.
	if r.lr.N > 0 {
		if _, err := io.Copy(io.Discard, &r.lr); err != nil {
			return nil, err
		}
	}

	// Read record header.
	buf := make([]byte, litestream.StreamRecordHeaderSize)
	if _, err := io.ReadFull(r.rc, buf); err != nil {
		return nil, fmt.Errorf("http.StreamReader.Next(): %w", err)
	}

	var hdr litestream.StreamRecordHeader
	if err := hdr.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	// Update remaining bytes on file reader.
	r.lr.N = hdr.Size

	return &hdr, nil
}
