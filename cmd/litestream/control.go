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

	"gopkg.in/yaml.v3"

	"github.com/benbjohnson/litestream"
)

// ControlServer manages runtime control via Unix socket and HTTP API.
type ControlServer struct {
	store      *litestream.Store
	config     *Config
	configPath string
	configLock sync.RWMutex

	socketPath     string
	socketPerms    uint32
	socketListener net.Listener

	httpMux *http.ServeMux

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	startedAt time.Time
	logger    *slog.Logger
}

// NewControlServer creates a new ControlServer instance.
func NewControlServer(store *litestream.Store, config *Config, configPath, socketPath string, socketPerms uint32, httpMux *http.ServeMux) *ControlServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &ControlServer{
		store:       store,
		config:      config,
		configPath:  configPath,
		socketPath:  socketPath,
		socketPerms: socketPerms,
		httpMux:     httpMux,
		ctx:         ctx,
		cancel:      cancel,
		startedAt:   time.Now(),
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
	case "list":
		return s.handleDatabases(req)
	case "info":
		return s.handleInfo(req)
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

	// Expand path for consistent comparison
	expandedPath, err := expand(params.Path)
	if err != nil {
		return newErrorResponse(req.ID, -32602, fmt.Sprintf("invalid path: %v", err), nil)
	}

	var dbConfig *DBConfig
	var dbConfigNode *yaml.Node

	// If config is provided, load and register the database.
	if params.Config != "" {
		var err error
		dbConfig, err = s.loadAndRegisterDB(expandedPath, params.Config)
		if err != nil {
			return newErrorResponse(req.ID, -32001, fmt.Sprintf("failed to load config: %v", err), nil)
		}
		dbConfigNode, err = LoadDBConfigNode(params.Config, expandedPath)
		if err != nil {
			return newErrorResponse(req.ID, -32001, fmt.Sprintf("failed to load config node: %v", err), nil)
		}
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

	if err := s.store.EnableDB(ctx, expandedPath); err != nil {
		return newErrorResponse(req.ID, -32001, err.Error(), nil)
	}

	// Persist to config if enabled
	if s.config != nil && s.config.Socket.PersistToConfig {
		if err := s.persistDBEnabled(expandedPath, true, dbConfig, dbConfigNode); err != nil {
			s.logger.Warn("failed to persist config", "error", err)
		}
	}

	if params.Wait {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return newErrorResponse(req.ID, -32004, "timeout waiting for start", nil)
			case <-ticker.C:
				db := s.store.FindDB(expandedPath)
				if db != nil && db.IsInitialized() {
					return newSuccessResponse(req.ID, map[string]interface{}{
						"status": "started",
						"path":   expandedPath,
					})
				}
			}
		}
	}

	return newSuccessResponse(req.ID, map[string]interface{}{
		"status": "started",
		"path":   expandedPath,
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

	// Expand path for consistent comparison
	expandedPath, err := expand(params.Path)
	if err != nil {
		return newErrorResponse(req.ID, -32602, fmt.Sprintf("invalid path: %v", err), nil)
	}

	if params.Timeout == 0 {
		params.Timeout = 30
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Duration(params.Timeout)*time.Second)
	defer cancel()

	if err := s.store.DisableDB(ctx, expandedPath); err != nil {
		return newErrorResponse(req.ID, -32001, err.Error(), nil)
	}

	// Persist to config if enabled
	if s.config != nil && s.config.Socket.PersistToConfig {
		if err := s.persistDBEnabled(expandedPath, false, nil, nil); err != nil {
			s.logger.Warn("failed to persist config", "error", err)
		}
	}

	return newSuccessResponse(req.ID, map[string]interface{}{
		"status": "stopped",
		"path":   expandedPath,
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

	expandedPath, err := expand(params.Path)
	if err != nil {
		return newErrorResponse(req.ID, -32602, fmt.Sprintf("invalid path: %v", err), nil)
	}

	db := s.store.FindDB(expandedPath)
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
		"path":   expandedPath,
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

	expandedPath, err := expand(params.Path)
	if err != nil {
		return newErrorResponse(req.ID, -32602, fmt.Sprintf("invalid path: %v", err), nil)
	}

	db := s.store.FindDB(expandedPath)
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
		"path":   expandedPath,
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

// handleInfo handles the info command.
func (s *ControlServer) handleInfo(req *RPCRequest) *RPCResponse {
	dbs := s.store.DBs()
	openCount := 0
	for _, db := range dbs {
		if db.IsOpen() {
			openCount++
		}
	}

	httpAddr := ""
	persistToConfig := false
	if s.config != nil {
		httpAddr = s.config.Addr
		persistToConfig = s.config.Socket.PersistToConfig
	}

	result := map[string]interface{}{
		"version":           Version,
		"pid":               os.Getpid(),
		"config_path":       s.configPath,
		"socket_path":       s.socketPath,
		"http_addr":         httpAddr,
		"persist_to_config": persistToConfig,
		"databases":         len(dbs),
		"open_databases":    openCount,
		"started_at":        s.startedAt.UTC().Format(time.RFC3339),
	}

	return newSuccessResponse(req.ID, result)
}

// loadAndRegisterDB loads a database configuration from a file and registers it with the store.
func (s *ControlServer) loadAndRegisterDB(dbPath, configPath string) (*DBConfig, error) {
	// Load config file
	config, err := ReadConfigFile(configPath, true)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	// Expand the requested db path for comparison
	expandedDBPath, err := expand(dbPath)
	if err != nil {
		return nil, fmt.Errorf("expand db path: %w", err)
	}

	// Find the database config that matches the requested path
	var dbConfig *DBConfig
	for _, dbc := range config.DBs {
		expandedPath, err := expand(dbc.Path)
		if err != nil {
			continue
		}
		if expandedPath == expandedDBPath {
			dbConfig = dbc
			break
		}
	}

	if dbConfig == nil {
		return nil, fmt.Errorf("database path %s not found in config file %s", dbPath, configPath)
	}

	// Create DB instance from config
	db, err := NewDBFromConfig(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("create database from config: %w", err)
	}

	// Apply store-level settings to the database
	// These are normally applied during store initialization
	db.L0Retention = s.store.L0Retention
	db.ShutdownSyncTimeout = s.store.ShutdownSyncTimeout
	db.ShutdownSyncInterval = s.store.ShutdownSyncInterval

	// Register the database with the store (doesn't open it)
	if err := s.store.RegisterDB(db); err != nil {
		return nil, fmt.Errorf("register database: %w", err)
	}

	return dbConfig, nil
}

// persistDBEnabled updates config file with enabled state.
func (s *ControlServer) persistDBEnabled(dbPath string, enabled bool, dbConfig *DBConfig, dbConfigNode *yaml.Node) error {
	s.configLock.Lock()
	defer s.configLock.Unlock()

	// Find DB config
	_, found := UpdateDBConfigInMemory(s.config, dbPath, func(dbc *DBConfig) {
		dbc.Enabled = &enabled
	})

	if found {
		// Write config atomically without expanding defaults.
		if _, err := WriteConfigEnabled(s.configPath, dbPath, enabled); err != nil {
			return fmt.Errorf("write config: %w", err)
		}
		s.logger.Info("persisted db state to config", "path", dbPath, "enabled", enabled)
		return nil
	}

	foundOnDisk, err := WriteConfigEnabled(s.configPath, dbPath, enabled)
	if err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if foundOnDisk {
		s.config.DBs = append(s.config.DBs, &DBConfig{
			Path:    dbPath,
			Enabled: &enabled,
		})
		s.logger.Info("persisted db state to config", "path", dbPath, "enabled", enabled)
		return nil
	}

	if dbConfig == nil || dbConfigNode == nil {
		// DB not in config - this is okay for dynamic DBs without config.
		return nil
	}

	dbCopy := *dbConfig
	dbCopy.Path = dbPath
	dbCopy.Enabled = &enabled
	s.config.DBs = append(s.config.DBs, &dbCopy)

	SetDBNodeEnabled(dbConfigNode, enabled)
	if err := WriteConfigAddDB(s.configPath, dbConfigNode); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	s.logger.Info("persisted db state to config", "path", dbPath, "enabled", enabled)
	return nil
}
