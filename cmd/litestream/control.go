package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
)

// Server manages runtime control via Unix socket using HTTP.
type Server struct {
	store          *litestream.Store
	socketPath     string
	socketPerms    uint32
	socketListener net.Listener
	httpServer     *http.Server

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger *slog.Logger
}

// NewServer creates a new Server instance.
func NewServer(store *litestream.Store, socketPath string, socketPerms uint32) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		store:       store,
		socketPath:  socketPath,
		socketPerms: socketPerms,
		ctx:         ctx,
		cancel:      cancel,
		logger:      slog.Default(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/start", s.handleStart)
	mux.HandleFunc("/stop", s.handleStop)

	s.httpServer = &http.Server{Handler: mux}

	return s
}

// Start begins listening for control connections.
func (s *Server) Start() error {
	if err := os.RemoveAll(s.socketPath); err != nil {
		return fmt.Errorf("remove existing socket: %w", err)
	}

	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("listen on unix socket: %w", err)
	}
	s.socketListener = listener

	if err := os.Chmod(s.socketPath, os.FileMode(s.socketPerms)); err != nil {
		listener.Close()
		return fmt.Errorf("chmod socket: %w", err)
	}

	s.logger.Info("control socket listening", "path", s.socketPath)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", "error", err)
		}
	}()

	return nil
}

// Close gracefully shuts down the control server.
func (s *Server) Close() error {
	s.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.logger.Error("http server shutdown error", "error", err)
		}
	}
	s.wg.Wait()
	return nil
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if req.Path == "" {
		writeJSONError(w, http.StatusBadRequest, "path required", nil)
		return
	}

	expandedPath, err := expand(req.Path)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err), nil)
		return
	}

	ctx := s.ctx
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(s.ctx, time.Duration(req.Timeout)*time.Second)
		defer cancel()
	}

	if err := s.store.EnableDB(ctx, expandedPath); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error(), nil)
		return
	}

	writeJSON(w, http.StatusOK, StartResponse{
		Status: "started",
		Path:   expandedPath,
	})
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StopRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if req.Path == "" {
		writeJSONError(w, http.StatusBadRequest, "path required", nil)
		return
	}

	expandedPath, err := expand(req.Path)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err), nil)
		return
	}

	timeout := req.Timeout
	if timeout == 0 {
		timeout = 30
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	if err := s.store.DisableDB(ctx, expandedPath); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error(), nil)
		return
	}

	writeJSON(w, http.StatusOK, StopResponse{
		Status: "stopped",
		Path:   expandedPath,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, message string, details interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error:   message,
		Details: details,
	})
}
