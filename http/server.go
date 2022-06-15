package http

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"strconv"
	"strings"

	"github.com/benbjohnson/litestream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

// Server represents an HTTP API server for Litestream.
type Server struct {
	ln     net.Listener
	closed bool

	httpServer  *http.Server
	promHandler http.Handler

	addr   string
	server *litestream.Server

	g errgroup.Group

	Logger *log.Logger
}

func NewServer(server *litestream.Server, addr string) *Server {
	s := &Server{
		addr:   addr,
		server: server,
		Logger: log.New(os.Stderr, "http: ", litestream.LogFlags),
	}

	s.promHandler = promhttp.Handler()
	s.httpServer = &http.Server{
		Handler: http.HandlerFunc(s.serveHTTP),
	}
	return s
}

func (s *Server) Open() (err error) {
	if s.ln, err = net.Listen("tcp", s.addr); err != nil {
		return err
	}

	s.g.Go(func() error {
		if err := s.httpServer.Serve(s.ln); err != nil && !s.closed {
			return err
		}
		return nil
	})

	return nil
}

func (s *Server) Close() (err error) {
	s.closed = true

	if s.ln != nil {
		if e := s.ln.Close(); e != nil && err == nil {
			err = e
		}
	}

	if e := s.g.Wait(); e != nil && err == nil {
		err = e
	}
	return err
}

// Port returns the port the listener is running on.
func (s *Server) Port() int {
	if s.ln == nil {
		return 0
	}
	return s.ln.Addr().(*net.TCPAddr).Port
}

// URL returns the full base URL for the running server.
func (s *Server) URL() string {
	host, _, _ := net.SplitHostPort(s.addr)
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s", net.JoinHostPort(host, fmt.Sprint(s.Port())))
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/debug/pprof") {
		switch r.URL.Path {
		case "/debug/pprof/cmdline":
			httppprof.Cmdline(w, r)
		case "/debug/pprof/profile":
			httppprof.Profile(w, r)
		case "/debug/pprof/symbol":
			httppprof.Symbol(w, r)
		case "/debug/pprof/trace":
			httppprof.Trace(w, r)
		default:
			httppprof.Index(w, r)
		}
		return
	}

	switch r.URL.Path {
	case "/metrics":
		s.promHandler.ServeHTTP(w, r)

	case "/stream":
		switch r.Method {
		case http.MethodGet:
			s.handleGetStream(w, r)
		default:
			s.writeError(w, r, "Method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleGetStream(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	path := q.Get("path")
	if path == "" {
		s.writeError(w, r, "Database name required", http.StatusBadRequest)
		return
	}

	generationStr := q.Get("generation")
	indexStr := q.Get("index")
	offsetStr := q.Get("offset")

	// Parse current client position, if available.
	var pos litestream.Pos
	if generationStr != "" && indexStr != "" && offsetStr != "" {

		index, err := litestream.ParseIndex(indexStr)
		if err != nil {
			s.writeError(w, r, "Invalid index query parameter", http.StatusBadRequest)
			return
		}

		offset, err := litestream.ParseOffset(offsetStr)
		if err != nil {
			s.writeError(w, r, "Invalid offset query parameter", http.StatusBadRequest)
			return
		}

		pos = litestream.Pos{
			Generation: generationStr,
			Index:      index,
			Offset:     offset,
		}
	}

	// Fetch database instance from the primary server.
	db := s.server.DB(path)
	if db == nil {
		s.writeError(w, r, "Database not found", http.StatusNotFound)
		return
	}

	// Set the page size in the header.
	w.Header().Set("Litestream-page-size", strconv.Itoa(db.PageSize()))

	// Determine starting position.
	dbPos := db.Pos()
	if dbPos.Generation == "" {
		s.writeError(w, r, "No generation available", http.StatusServiceUnavailable)
		return
	}

	// Use database position if generation has changed.
	var snapshotRequired bool
	if pos.Generation != dbPos.Generation {
		s.Logger.Printf("stream generation mismatch, using primary position: client.pos=%s", pos)
		pos, snapshotRequired = dbPos, true
	}
	pos.Offset = 0

	// Obtain iterator before snapshot so we don't miss any WAL segments.
	fitr, err := db.WALSegments(r.Context(), pos.Generation)
	if err != nil {
		s.writeError(w, r, fmt.Sprintf("Cannot obtain WAL iterator: %s", err), http.StatusInternalServerError)
		return
	}
	defer fitr.Close()

	bitr := litestream.NewBufferedWALSegmentIterator(fitr)

	// Peek at first position to see if client is too old.
	if info, ok := bitr.Peek(); !ok {
		s.writeError(w, r, "cannot peek WAL iterator, no segments available", http.StatusInternalServerError)
		return
	} else if cmp, err := litestream.ComparePos(pos, info.Pos()); err != nil {
		s.writeError(w, r, fmt.Sprintf("cannot compare pos: %s", err), http.StatusInternalServerError)
		return
	} else if cmp == -1 {
		s.Logger.Printf("stream position no longer available, using using primary position: client.pos=%s", pos)
		pos, snapshotRequired = dbPos, true
	}

	s.Logger.Printf("stream connected: pos=%s snapshot=%v", pos, snapshotRequired)
	defer s.Logger.Printf("stream disconnected")

	// Write snapshot to response body.
	if snapshotRequired {
		if err := db.WithFile(func(f *os.File) error {
			fi, err := f.Stat()
			if err != nil {
				return err
			}

			// Write snapshot header with current position & size.
			hdr := litestream.StreamRecordHeader{
				Type:       litestream.StreamRecordTypeSnapshot,
				Generation: pos.Generation,
				Index:      pos.Index,
				Size:       fi.Size(),
			}
			if buf, err := hdr.MarshalBinary(); err != nil {
				return fmt.Errorf("marshal snapshot stream record header: %w", err)
			} else if _, err := w.Write(buf); err != nil {
				return fmt.Errorf("write snapshot stream record header: %w", err)
			}

			if _, err := io.CopyN(w, f, fi.Size()); err != nil {
				return fmt.Errorf("copy snapshot: %w", err)
			}

			return nil
		}); err != nil {
			s.writeError(w, r, err.Error(), http.StatusInternalServerError)
			return
		}

		// Flush after snapshot has been written.
		w.(http.Flusher).Flush()
	}

	for {
		// Wait for notification of new entries.
		select {
		case <-r.Context().Done():
			return
		case <-fitr.NotifyCh():
		}

		for bitr.Next() {
			info := bitr.WALSegment()

			// Skip any segments before our initial position.
			if cmp, err := litestream.ComparePos(info.Pos(), pos); err != nil {
				s.Logger.Printf("pos compare: %s", err)
				return
			} else if cmp == -1 {
				continue
			}

			hdr := litestream.StreamRecordHeader{
				Type:       litestream.StreamRecordTypeWALSegment,
				Flags:      0,
				Generation: info.Generation,
				Index:      info.Index,
				Offset:     info.Offset,
				Size:       info.Size,
			}

			// Write record header.
			data, err := hdr.MarshalBinary()
			if err != nil {
				s.Logger.Printf("marshal WAL segment stream record header: %s", err)
				return
			} else if _, err := w.Write(data); err != nil {
				s.Logger.Printf("write WAL segment stream record header: %s", err)
				return
			}

			// Copy WAL segment data to writer.
			if err := func() error {
				rd, err := db.WALSegmentReader(r.Context(), info.Pos())
				if err != nil {
					return fmt.Errorf("cannot fetch wal segment reader: %w", err)
				}
				defer rd.Close()

				if _, err := io.CopyN(w, rd, hdr.Size); err != nil {
					return fmt.Errorf("cannot copy wal segment: %w", err)
				}
				return nil
			}(); err != nil {
				log.Print(err)
				return
			}

			// Flush after WAL segment has been written.
			w.(http.Flusher).Flush()
		}
		if err := bitr.Err(); err != nil {
			s.Logger.Printf("wal iterator error: %s", err)
			return
		}
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err string, code int) {
	s.Logger.Printf("error: %s", err)
	http.Error(w, err, code)
}
