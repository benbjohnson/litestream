package litestream

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/benbjohnson/litestream/internal"
	"github.com/pierrec/lz4/v4"
	"golang.org/x/sync/errgroup"
)

// WALDownloader represents a parallel downloader of WAL files from a replica client.
//
// It works on a per-index level so WAL files are always downloaded in their
// entiretry and are not segmented. WAL files are downloaded from minIndex to
// maxIndex, inclusively, and are written to a path prefix. WAL files are named
// with the prefix and suffixed with the WAL index. It is the responsibility of
// the caller to clean up these WAL files.
//
// The purpose of the parallelization is that RTT & WAL apply time can consume
// much of the restore time so it's useful to download multiple WAL files in
// the background to minimize the latency. While some WAL indexes may be
// downloaded out of order, the WALDownloader ensures that Next() always
// returns the WAL files sequentially.
type WALDownloader struct {
	ctx    context.Context // context used for early close/cancellation
	cancel func()

	client     ReplicaClient // client to read WAL segments with
	generation string        // generation to download WAL files from
	minIndex   int           // starting WAL index (inclusive)
	maxIndex   int           // ending WAL index (inclusive)
	prefix     string        // output file prefix

	err error // error occuring during init, propagated to Next()
	n   int   // number of WAL files returned by Next()

	// Concurrency coordination
	mu        sync.Mutex             // used to serialize sending of next WAL index
	cond      *sync.Cond             // used with mu above
	g         *errgroup.Group        // manages worker goroutines for downloading
	input     chan walDownloadInput  // holds ordered WAL indices w/ offsets
	output    chan walDownloadOutput // always sends next sequential WAL; used by Next()
	nextIndex int                    // tracks next WAL index to send to output channel

	// File info used for downloaded WAL files.
	Mode     os.FileMode
	Uid, Gid int

	// Number of downloads occurring in parallel.
	Parallelism int
}

// NewWALDownloader returns a new instance of WALDownloader.
func NewWALDownloader(client ReplicaClient, prefix string, generation string, minIndex, maxIndex int) *WALDownloader {
	d := &WALDownloader{
		client:     client,
		prefix:     prefix,
		generation: generation,
		minIndex:   minIndex,
		maxIndex:   maxIndex,

		Mode:        0600,
		Parallelism: 1,
	}

	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.cond = sync.NewCond(&d.mu)

	return d
}

// Close cancels all downloads and returns any error that has occurred.
func (d *WALDownloader) Close() (err error) {
	if d.err != nil {
		err = d.err
	}

	d.cancel()

	if d.g != nil {
		if e := d.g.Wait(); err != nil && e != context.Canceled {
			err = e
		}
	}
	return err
}

// init initializes the downloader on the first invocation only. It generates
// the input channel with all WAL indices & offsets needed, it initializes
// the output channel that Next() waits on, and starts the worker goroutines
// that begin downloading WAL files in the background.
func (d *WALDownloader) init(ctx context.Context) error {
	if d.input != nil {
		return nil // already initialized
	} else if d.minIndex < 0 {
		return fmt.Errorf("minimum index required")
	} else if d.maxIndex < 0 {
		return fmt.Errorf("maximum index required")
	} else if d.maxIndex < d.minIndex {
		return fmt.Errorf("minimum index cannot be larger than maximum index")
	} else if d.Parallelism < 1 {
		return fmt.Errorf("parallelism must be at least one")
	}

	// Populate input channel with indices & offsets.
	if err := d.initInputCh(ctx); err != nil {
		return err
	}
	d.nextIndex = d.minIndex

	// Generate output channel that Next() pulls from.
	d.output = make(chan walDownloadOutput)

	// Spawn worker goroutines to download WALs.
	d.g, d.ctx = errgroup.WithContext(d.ctx)
	for i := 0; i < d.Parallelism; i++ {
		d.g.Go(func() error { return d.downloader(d.ctx) })
	}

	return nil
}

// initInputCh populates the input channel with each WAL index between minIndex
// and maxIndex. It also includes all offsets needed with the index.
func (d *WALDownloader) initInputCh(ctx context.Context) error {
	itr, err := d.client.WALSegments(ctx, d.generation)
	if err != nil {
		return fmt.Errorf("wal segments: %w", err)
	}
	defer func() { _ = itr.Close() }()

	d.input = make(chan walDownloadInput, d.maxIndex-d.minIndex+1)
	defer close(d.input)

	index := d.minIndex - 1
	var offsets []int64
	for itr.Next() {
		info := itr.WALSegment()

		// Restrict segments to within our index range.
		if info.Index < d.minIndex {
			continue // haven't reached minimum index, skip
		} else if info.Index > d.maxIndex {
			break // after max index, stop
		}

		// Flush index & offsets when index changes.
		if info.Index != index {
			if info.Index != index+1 { // must be sequential
				return &WALNotFoundError{Generation: d.generation, Index: index + 1}
			}

			if len(offsets) > 0 {
				d.input <- walDownloadInput{index: index, offsets: offsets}
				offsets = make([]int64, 0)
			}

			index = info.Index
		}

		// Append to the end of the WAL file.
		offsets = append(offsets, info.Offset)
	}

	// Ensure we read to the last index.
	if index != d.maxIndex {
		return &WALNotFoundError{Generation: d.generation, Index: index + 1}
	}

	// Flush if we have remaining offsets.
	if len(offsets) > 0 {
		d.input <- walDownloadInput{index: index, offsets: offsets}
	}

	return itr.Close()
}

// N returns the number of WAL files returned by Next().
func (d *WALDownloader) N() int { return d.n }

// Next returns the index & local file path of the next downloaded WAL file.
func (d *WALDownloader) Next(ctx context.Context) (int, string, error) {
	if d.err != nil {
		return 0, "", d.err
	} else if d.err = d.init(ctx); d.err != nil {
		return 0, "", d.err
	}

	select {
	case <-ctx.Done():
		return 0, "", ctx.Err()
	case <-d.ctx.Done():
		return 0, "", d.ctx.Err()
	case v, ok := <-d.output:
		if !ok {
			return 0, "", io.EOF
		}

		d.n++
		return v.index, v.path, v.err
	}
}

// downloader runs in a separate goroutine and downloads the next input index.
func (d *WALDownloader) downloader(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			d.cond.Broadcast()
			return ctx.Err()

		case input, ok := <-d.input:
			if !ok {
				return nil // no more input
			}

			// Wait until next index equals input index and then send file to
			// output to ensure sorted order.
			if err := func() error {
				walPath, err := d.downloadWAL(ctx, input.index, input.offsets)

				d.mu.Lock()
				defer d.mu.Unlock()

				// Notify other downloader goroutines when we escape this
				// anonymous function.
				defer d.cond.Broadcast()

				// Keep looping until our index matches the next index to send.
				for d.nextIndex != input.index {
					if ctxErr := ctx.Err(); ctxErr != nil {
						return ctxErr
					}
					d.cond.Wait()
				}

				// Still under lock, wait until Next() requests next index.
				select {
				case <-ctx.Done():
					return ctx.Err()

				case d.output <- walDownloadOutput{
					index: input.index,
					path:  walPath,
					err:   err,
				}:
					// At the last index, close out output channel to notify
					// the Next() method to return io.EOF.
					if d.nextIndex == d.maxIndex {
						close(d.output)
						return nil
					}

					// Update next expected index now that our send is successful.
					d.nextIndex++
				}

				return err
			}(); err != nil {
				return err
			}
		}
	}
}

// downloadWAL sequentially downloads all the segments for WAL index from the
// replica client and appends them to a single on-disk file. Returns the name
// of the on-disk file on success.
func (d *WALDownloader) downloadWAL(ctx context.Context, index int, offsets []int64) (string, error) {
	// Open handle to destination WAL path.
	walPath := fmt.Sprintf("%s-%s-wal", d.prefix, FormatIndex(index))
	f, err := internal.CreateFile(walPath, d.Mode, d.Uid, d.Gid)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Open readers for every segment in the WAL file, in order.
	var written int64
	for _, offset := range offsets {
		if err := func() error {
			// Ensure next offset is our current position in the file.
			if written != offset {
				return fmt.Errorf("missing WAL offset: generation=%s index=%s offset=%s", d.generation, FormatIndex(index), FormatOffset(written))
			}

			rd, err := d.client.WALSegmentReader(ctx, Pos{Generation: d.generation, Index: index, Offset: offset})
			if err != nil {
				return fmt.Errorf("read WAL segment: %w", err)
			}
			defer rd.Close()

			n, err := io.Copy(f, lz4.NewReader(rd))
			if err != nil {
				return fmt.Errorf("copy WAL segment: %w", err)
			}
			written += n

			return nil
		}(); err != nil {
			return "", err
		}
	}

	if err := f.Close(); err != nil {
		return "", err
	}
	return walPath, nil
}

type walDownloadInput struct {
	index   int
	offsets []int64
}

type walDownloadOutput struct {
	path  string
	index int
	err   error
}

// WALNotFoundError is returned by WALDownloader if an WAL index is not found.
type WALNotFoundError struct {
	Generation string
	Index      int
}

// Error returns the error string.
func (e *WALNotFoundError) Error() string {
	return fmt.Sprintf("wal not found: generation=%s index=%s", e.Generation, FormatIndex(e.Index))
}
