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

// ControlServer manages runtime control via Unix socket and HTTP API.
type ControlServer struct {
	store      *litestream.Store
	configPath string

	socketPath     string
	socketPerms    uint32
	socketListener net.Listener

	httpMux *http.ServeMux

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger *slog.Logger
}

// NewControlServer creates a new ControlServer instance.
func NewControlServer(store *litestream.Store, configPath, socketPath string, socketPerms uint32, httpMux *http.ServeMux) *ControlServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &ControlServer{
		store:       store,
		configPath:  configPath,
		socketPath:  socketPath,
		socketPerms: socketPerms,
		httpMux:     httpMux,
		ctx:         ctx,
		cancel:      cancel,
		logger:      slog.Default(),
	}
}

// Start begins listening for control connections.
func (s *ControlServer) Start() error {
	if s.socketPath != "" {
		if err := s.startUnixSocket(); err != nil {
			return fmt.Errorf("start unix socket: %w", err)
		}
	}

	if s.httpMux != nil {
		s.httpMux.HandleFunc("/control/", s.handleHTTP)
	}

	return nil
}

// Close gracefully shuts down the control server.
func (s *ControlServer) Close() error {
	s.cancel()

	if s.socketListener != nil {
		s.socketListener.Close()
	}

	s.wg.Wait()
	return nil
}

// startUnixSocket starts listening on the Unix socket.
func (s *ControlServer) startUnixSocket() error {
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
	go s.acceptLoop(listener)

	return nil
}

// acceptLoop accepts incoming connections on the Unix socket.
func (s *ControlServer) acceptLoop(listener net.Listener) {
	defer s.wg.Done()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.logger.Error("accept error", "error", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single socket connection.
func (s *ControlServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	var req RPCRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		s.logger.Error("decode request", "error", err)
		return
	}

	resp := s.handleRequest(&req)

	if err := json.NewEncoder(conn).Encode(resp); err != nil {
		s.logger.Error("encode response", "error", err)
	}
}

// handleHTTP handles HTTP requests to the control API.
func (s *ControlServer) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	resp := s.handleRequest(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleRequest processes an RPC request and returns a response.
func (s *ControlServer) handleRequest(req *RPCRequest) *RPCResponse {
	switch req.Method {
	case "start":
		return s.handleStart(req)
	case "stop":
		return s.handleStop(req)
	case "sync":
		return s.handleSync(req)
	case "status":
		return s.handleStatus(req)
	case "databases":
		return s.handleDatabases(req)
	default:
		return newErrorResponse(req.ID, -32601, "Method not found", nil)
	}
}

// handleStart handles the start command.
func (s *ControlServer) handleStart(req *RPCRequest) *RPCResponse {
	var params StartParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return newErrorResponse(req.ID, -32602, "Invalid params", err.Error())
	}

	if params.Path == "" {
		return newErrorResponse(req.ID, -32602, "path required", nil)
	}

	ctx := s.ctx
	if params.Wait {
		if params.Timeout == 0 {
			params.Timeout = 30
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(s.ctx, time.Duration(params.Timeout)*time.Second)
		defer cancel()
	}

	if err := s.store.EnableDB(ctx, params.Path); err != nil {
		return newErrorResponse(req.ID, -32001, err.Error(), nil)
	}

	if params.Wait {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return newErrorResponse(req.ID, -32004, "timeout waiting for start", nil)
			case <-ticker.C:
				db := s.store.FindDB(params.Path)
				if db != nil && db.IsOpen() {
					return newSuccessResponse(req.ID, map[string]interface{}{
						"status": "started",
						"path":   params.Path,
					})
				}
			}
		}
	}

	return newSuccessResponse(req.ID, map[string]interface{}{
		"status": "started",
		"path":   params.Path,
	})
}

// handleStop handles the stop command.
func (s *ControlServer) handleStop(req *RPCRequest) *RPCResponse {
	var params StopParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return newErrorResponse(req.ID, -32602, "Invalid params", err.Error())
	}

	if params.Path == "" {
		return newErrorResponse(req.ID, -32602, "path required", nil)
	}

	ctx := s.ctx
	if params.Wait {
		if params.Timeout == 0 {
			params.Timeout = 30
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(s.ctx, time.Duration(params.Timeout)*time.Second)
		defer cancel()
	}

	if err := s.store.DisableDB(ctx, params.Path); err != nil {
		return newErrorResponse(req.ID, -32001, err.Error(), nil)
	}

	return newSuccessResponse(req.ID, map[string]interface{}{
		"status": "stopped",
		"path":   params.Path,
	})
}

// handleSync handles the sync command.
func (s *ControlServer) handleSync(req *RPCRequest) *RPCResponse {
	var params SyncParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return newErrorResponse(req.ID, -32602, "Invalid params", err.Error())
	}

	if params.Path == "" {
		return newErrorResponse(req.ID, -32602, "path required", nil)
	}

	db := s.store.FindDB(params.Path)
	if db == nil {
		return newErrorResponse(req.ID, -32001, "database not found", nil)
	}

	if db.Replica == nil {
		return newErrorResponse(req.ID, -32001, "database has no replica", nil)
	}

	ctx := s.ctx
	if params.Wait {
		if params.Timeout == 0 {
			params.Timeout = 30
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(s.ctx, time.Duration(params.Timeout)*time.Second)
		defer cancel()
	}

	if err := db.Replica.Sync(ctx); err != nil {
		return newErrorResponse(req.ID, -32001, fmt.Sprintf("sync failed: %v", err), nil)
	}

	return newSuccessResponse(req.ID, map[string]interface{}{
		"status": "synced",
		"path":   params.Path,
	})
}

// handleStatus handles the status command.
func (s *ControlServer) handleStatus(req *RPCRequest) *RPCResponse {
	var params StatusParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return newErrorResponse(req.ID, -32602, "Invalid params", err.Error())
	}

	if params.Path == "" {
		return newErrorResponse(req.ID, -32602, "path required", nil)
	}

	db := s.store.FindDB(params.Path)
	if db == nil {
		return newErrorResponse(req.ID, -32001, "database not found", nil)
	}

	status := "unknown"
	if db.IsOpen() {
		status = "open"
	} else {
		status = "closed"
	}

	return newSuccessResponse(req.ID, map[string]interface{}{
		"path":   params.Path,
		"status": status,
	})
}

// handleDatabases handles the databases command.
func (s *ControlServer) handleDatabases(req *RPCRequest) *RPCResponse {
	dbs := s.store.DBs()
	result := make([]map[string]interface{}, len(dbs))

	for i, db := range dbs {
		result[i] = map[string]interface{}{
			"path": db.Path(),
			"status": func() string {
				if db.IsOpen() {
					return "open"
				}
				return "closed"
			}(),
		}
	}

	return newSuccessResponse(req.ID, result)
}
