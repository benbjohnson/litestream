package http

import (
	"fmt"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
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
	default:
		http.NotFound(w, r)
	}
}
