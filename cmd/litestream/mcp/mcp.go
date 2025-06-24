package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/MadAppGang/httplog"
	"github.com/benbjohnson/litestream/cmd/litestream/mcp/tools"
	"github.com/mark3labs/mcp-go/server"
)

type Server struct {
	ctx        context.Context
	mux        *http.ServeMux
	httpServer *http.Server
	configPath string
}

func New(ctx context.Context, configPath string) (*Server, error) {
	s := &Server{
		ctx:        ctx,
		configPath: configPath,
	}

	mcpServer := tools.NewServer(configPath)

	s.mux = http.NewServeMux()
	s.mux.Handle("/", httplog.Logger(server.NewStreamableHTTPServer(mcpServer)))
	return s, nil
}

func (s *Server) Start(port int) {
	addr := fmt.Sprintf(":%d", port)
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.mux,
	}
	go func() {
		slog.Info("Starting MCP Streamable HTTP server", "addr", addr)
		if err := s.httpServer.ListenAndServe(); err != nil {
			slog.Error("MCP server error", "error", err)
		}
	}()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close attemps to gracefully shutdown the server.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
