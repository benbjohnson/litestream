package litestream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
	"time"
)

// SocketConfig configures the Unix socket for control commands.
type SocketConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Path        string `yaml:"path"`
	Permissions uint32 `yaml:"permissions"`
}

// DefaultSocketConfig returns the default socket configuration.
func DefaultSocketConfig() SocketConfig {
	return SocketConfig{
		Enabled:     false,
		Path:        "/var/run/litestream.sock",
		Permissions: 0600,
	}
}

// Server manages runtime control via Unix socket using HTTP.
type Server struct {
	store *Store

	// SocketPath is the path to the Unix socket.
	SocketPath string

	// SocketPerms is the file permissions for the socket.
	SocketPerms uint32

	// PathExpander optionally expands paths (e.g., ~ expansion).
	// If nil, paths are used as-is.
	PathExpander func(string) (string, error)

	// Version is the version string to report in /info.
	Version string

	// startedAt is set when the server starts.
	startedAt time.Time

	socketListener net.Listener
	httpServer     *http.Server

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger *slog.Logger
}

// NewServer creates a new Server instance.
func NewServer(store *Store) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		store:       store,
		SocketPerms: 0600,
		ctx:         ctx,
		cancel:      cancel,
		logger:      slog.Default(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /start", s.handleStart)
	mux.HandleFunc("POST /stop", s.handleStop)
	mux.HandleFunc("GET /txid", s.handleTXID)
	mux.HandleFunc("POST /add", s.handleAdd)
	mux.HandleFunc("POST /remove", s.handleRemove)
	mux.HandleFunc("GET /list", s.handleList)
	mux.HandleFunc("GET /info", s.handleInfo)

	// pprof endpoints
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	s.httpServer = &http.Server{Handler: mux}

	return s
}

// Start begins listening for control connections.
func (s *Server) Start() error {
	if s.SocketPath == "" {
		return fmt.Errorf("socket path required")
	}

	// Check if socket file exists and is actually a socket before removing
	if info, err := os.Lstat(s.SocketPath); err == nil {
		if info.Mode()&os.ModeSocket != 0 {
			if err := os.Remove(s.SocketPath); err != nil {
				return fmt.Errorf("remove existing socket: %w", err)
			}
		} else {
			return fmt.Errorf("socket path exists but is not a socket: %s", s.SocketPath)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("check socket path: %w", err)
	}

	listener, err := net.Listen("unix", s.SocketPath)
	if err != nil {
		return fmt.Errorf("listen on unix socket: %w", err)
	}
	s.socketListener = listener

	if err := os.Chmod(s.SocketPath, os.FileMode(s.SocketPerms)); err != nil {
		listener.Close()
		return fmt.Errorf("chmod socket: %w", err)
	}

	// Set startedAt after successful socket setup to ensure uptime reflects
	// the actual time the server became available.
	s.startedAt = time.Now()

	s.logger.Info("control socket listening", "path", s.SocketPath)

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

// expandPath expands the path using PathExpander if set.
func (s *Server) expandPath(path string) (string, error) {
	if s.PathExpander != nil {
		return s.PathExpander(path)
	}
	return path, nil
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if req.Path == "" {
		writeJSONError(w, http.StatusBadRequest, "path required", nil)
		return
	}

	expandedPath, err := s.expandPath(req.Path)
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
	var req StopRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if req.Path == "" {
		writeJSONError(w, http.StatusBadRequest, "path required", nil)
		return
	}

	expandedPath, err := s.expandPath(req.Path)
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

func (s *Server) handleTXID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeJSONError(w, http.StatusBadRequest, "path required", nil)
		return
	}

	expandedPath, err := s.expandPath(path)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err), nil)
		return
	}

	db := s.store.FindDB(expandedPath)
	if db == nil {
		writeJSONError(w, http.StatusNotFound, "database not found", nil)
		return
	}

	pos, err := db.Pos()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error(), nil)
		return
	}

	writeJSON(w, http.StatusOK, TXIDResponse{
		TXID: uint64(pos.TXID),
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

// StartRequest is the request body for the /start endpoint.
type StartRequest struct {
	Path    string `json:"path"`
	Timeout int    `json:"timeout,omitempty"`
}

// StartResponse is the response body for the /start endpoint.
type StartResponse struct {
	Status string `json:"status"`
	Path   string `json:"path"`
}

// StopRequest is the request body for the /stop endpoint.
type StopRequest struct {
	Path    string `json:"path"`
	Timeout int    `json:"timeout,omitempty"`
}

// StopResponse is the response body for the /stop endpoint.
type StopResponse struct {
	Status string `json:"status"`
	Path   string `json:"path"`
}

// ErrorResponse is returned when an error occurs.
type ErrorResponse struct {
	Error   string      `json:"error"`
	Details interface{} `json:"details,omitempty"`
}

// TXIDResponse is the response body for the /txid endpoint.
type TXIDResponse struct {
	TXID uint64 `json:"txid"`
}

func (s *Server) handleList(w http.ResponseWriter, _ *http.Request) {
	dbs := s.store.DBs()
	resp := ListResponse{
		Databases: make([]DatabaseSummary, 0, len(dbs)),
	}

	for _, db := range dbs {
		var status string
		if db.IsOpen() {
			if db.Replica != nil && db.Replica.MonitorEnabled {
				status = "replicating"
			} else {
				status = "open"
			}
		} else {
			status = "stopped"
		}

		summary := DatabaseSummary{
			Path:   db.Path(),
			Status: status,
		}

		if t := db.LastSuccessfulSyncAt(); !t.IsZero() {
			summary.LastSyncAt = &t
		}

		resp.Databases = append(resp.Databases, summary)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	resp := InfoResponse{
		Version:       s.Version,
		PID:           os.Getpid(),
		StartedAt:     s.startedAt,
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		DatabaseCount: len(s.store.DBs()),
	}

	writeJSON(w, http.StatusOK, resp)
}

// ListResponse is the response body for the /list endpoint.
type ListResponse struct {
	Databases []DatabaseSummary `json:"databases"`
}

// DatabaseSummary contains summary information about a database.
type DatabaseSummary struct {
	Path   string `json:"path"`
	Status string `json:"status"`

	// LastSyncAt is the timestamp of the last successful replica sync.
	// This reflects when data was last successfully uploaded to the replica
	// storage backend, not just when the local WAL was processed.
	LastSyncAt *time.Time `json:"last_sync_at,omitempty"`
}

// InfoResponse is the response body for the /info endpoint.
type InfoResponse struct {
	Version       string    `json:"version"`
	PID           int       `json:"pid"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	StartedAt     time.Time `json:"started_at"`
	DatabaseCount int       `json:"database_count"`
}

// AddDatabaseRequest is the request body for the /add endpoint.
type AddDatabaseRequest struct {
	Path       string `json:"path"`
	ReplicaURL string `json:"replica_url"`
}

// AddDatabaseResponse is the response body for the /add endpoint.
type AddDatabaseResponse struct {
	Status string `json:"status"`
	Path   string `json:"path"`
}

// RemoveDatabaseRequest is the request body for the /remove endpoint.
type RemoveDatabaseRequest struct {
	Path    string `json:"path"`
	Timeout int    `json:"timeout,omitempty"`
}

// RemoveDatabaseResponse is the response body for the /remove endpoint.
type RemoveDatabaseResponse struct {
	Status string `json:"status"`
	Path   string `json:"path"`
}

func (s *Server) handleAdd(w http.ResponseWriter, r *http.Request) {
	var req AddDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if req.Path == "" {
		writeJSONError(w, http.StatusBadRequest, "path required", nil)
		return
	}

	if req.ReplicaURL == "" {
		writeJSONError(w, http.StatusBadRequest, "replica_url required", nil)
		return
	}

	expandedPath, err := s.expandPath(req.Path)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err), nil)
		return
	}

	// Check if database already exists.
	if existing := s.store.FindDB(expandedPath); existing != nil {
		writeJSON(w, http.StatusOK, AddDatabaseResponse{
			Status: "already_exists",
			Path:   expandedPath,
		})
		return
	}

	// Create replica client from URL.
	client, err := NewReplicaClientFromURL(req.ReplicaURL)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid replica url: %v", err), nil)
		return
	}

	// Create new database.
	db := NewDB(expandedPath)

	// Create replica and attach client.
	replica := NewReplica(db)
	replica.Client = client
	db.Replica = replica

	// Register database with store (this also opens the database).
	if err := s.store.AddDB(db); err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to add database: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, AddDatabaseResponse{
		Status: "added",
		Path:   expandedPath,
	})
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	var req RemoveDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if req.Path == "" {
		writeJSONError(w, http.StatusBadRequest, "path required", nil)
		return
	}

	expandedPath, err := s.expandPath(req.Path)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid path: %v", err), nil)
		return
	}

	// Set up timeout context.
	timeout := req.Timeout
	if timeout == 0 {
		timeout = 30
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	// Remove database from store (this also closes it).
	if err := s.store.RemoveDB(ctx, expandedPath); err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to remove database: %v", err), nil)
		return
	}

	writeJSON(w, http.StatusOK, RemoveDatabaseResponse{
		Status: "removed",
		Path:   expandedPath,
	})
}
