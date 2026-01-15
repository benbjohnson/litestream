package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

func TestControlServer_loadAndRegisterDB(t *testing.T) {
	t.Run("LoadValidConfig", func(t *testing.T) {
		// Create temp directory for test
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		configPath := filepath.Join(tmpDir, "config.yml")

		// Write a minimal config file
		configContent := `dbs:
  - path: ` + dbPath + `
    replicas:
      - url: file://` + filepath.Join(tmpDir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		// Create control server with store
		store := litestream.NewStore(nil, nil)
		controlServer := &ControlServer{
			store: store,
			ctx:   context.Background(),
		}

		// Load and register DB
		_, err := controlServer.loadAndRegisterDB(dbPath, configPath)
		if err != nil {
			t.Fatalf("loadAndRegisterDB failed: %v", err)
		}

		// Verify DB was registered
		db := store.FindDB(dbPath)
		if db == nil {
			t.Fatal("expected database to be registered")
		}

		if db.Path() != dbPath {
			t.Fatalf("expected path %s, got %s", dbPath, db.Path())
		}

		if db.Replica == nil {
			t.Fatal("expected replica to be configured")
		}
	})

	t.Run("ErrorOnNonexistentConfig", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		configPath := filepath.Join(tmpDir, "nonexistent.yml")

		store := litestream.NewStore(nil, nil)
		controlServer := &ControlServer{
			store: store,
			ctx:   context.Background(),
		}

		_, err := controlServer.loadAndRegisterDB(dbPath, configPath)
		if err == nil {
			t.Fatal("expected error for nonexistent config file")
		}
	})

	t.Run("ErrorOnDatabaseNotInConfig", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		otherPath := filepath.Join(tmpDir, "other.db")
		configPath := filepath.Join(tmpDir, "config.yml")

		// Config file has a different database path
		configContent := `dbs:
  - path: ` + otherPath + `
    replicas:
      - url: file://` + filepath.Join(tmpDir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		store := litestream.NewStore(nil, nil)
		controlServer := &ControlServer{
			store: store,
			ctx:   context.Background(),
		}

		_, err := controlServer.loadAndRegisterDB(dbPath, configPath)
		if err == nil {
			t.Fatal("expected error when database not found in config")
		}
	})
}

func TestControlServer_handleHTTP_ListInfo(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath1 := filepath.Join(tmpDir, "one.db")
	dbPath2 := filepath.Join(tmpDir, "two.db")

	db1 := litestream.NewDB(dbPath1)
	db1.MonitorInterval = 0
	if err := os.MkdirAll(db1.MetaPath(), 0o755); err != nil {
		t.Fatalf("create meta path: %v", err)
	}
	if err := db1.Open(); err != nil {
		t.Fatalf("open db1: %v", err)
	}
	t.Cleanup(func() { _ = db1.Close(context.Background()) })

	db2 := litestream.NewDB(dbPath2)
	db2.MonitorInterval = 0

	store := litestream.NewStore([]*litestream.DB{db1, db2}, nil)
	config := &Config{
		Addr:            "127.0.0.1:9090",
		PersistToConfig: true,
	}
	controlServer := NewControlServer(store, config, "/tmp/litestream.yml", "/tmp/litestream.sock", 0600, nil)

	doRequest := func(t *testing.T, method string) RPCResponse {
		t.Helper()
		req := RPCRequest{
			JSONRPC: "2.0",
			Method:  method,
			Params:  json.RawMessage(`{}`),
			ID:      1,
		}
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}

		httpReq := httptest.NewRequest(http.MethodPost, "/control/", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		controlServer.handleHTTP(rec, httpReq)

		var resp RPCResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return resp
	}

	t.Run("List", func(t *testing.T) {
		resp := doRequest(t, "list")
		if resp.Error != nil {
			t.Fatalf("list error: %v", resp.Error.Message)
		}

		items, ok := resp.Result.([]interface{})
		if !ok {
			t.Fatalf("expected list result slice, got %T", resp.Result)
		}
		if len(items) != 2 {
			t.Fatalf("unexpected list length: %d", len(items))
		}

		item0, ok := items[0].(map[string]interface{})
		if !ok {
			t.Fatalf("expected list item map, got %T", items[0])
		}
		if got, want := item0["path"], dbPath1; got != want {
			t.Fatalf("first path: got %v, want %v", got, want)
		}
		if got, want := item0["status"], "open"; got != want {
			t.Fatalf("first status: got %v, want %v", got, want)
		}

		item1, ok := items[1].(map[string]interface{})
		if !ok {
			t.Fatalf("expected list item map, got %T", items[1])
		}
		if got, want := item1["path"], dbPath2; got != want {
			t.Fatalf("second path: got %v, want %v", got, want)
		}
		if got, want := item1["status"], "closed"; got != want {
			t.Fatalf("second status: got %v, want %v", got, want)
		}
	})

	t.Run("DatabasesAlias", func(t *testing.T) {
		resp := doRequest(t, "databases")
		if resp.Error != nil {
			t.Fatalf("databases error: %v", resp.Error.Message)
		}
		items, ok := resp.Result.([]interface{})
		if !ok {
			t.Fatalf("expected list result slice, got %T", resp.Result)
		}
		if len(items) != 2 {
			t.Fatalf("unexpected list length: %d", len(items))
		}
	})

	t.Run("Info", func(t *testing.T) {
		resp := doRequest(t, "info")
		if resp.Error != nil {
			t.Fatalf("info error: %v", resp.Error.Message)
		}

		info, ok := resp.Result.(map[string]interface{})
		if !ok {
			t.Fatalf("expected info result map, got %T", resp.Result)
		}

		if got, want := info["config_path"], "/tmp/litestream.yml"; got != want {
			t.Fatalf("config_path: got %v, want %v", got, want)
		}
		if got, want := info["socket_path"], "/tmp/litestream.sock"; got != want {
			t.Fatalf("socket_path: got %v, want %v", got, want)
		}
		if got, want := info["http_addr"], "127.0.0.1:9090"; got != want {
			t.Fatalf("http_addr: got %v, want %v", got, want)
		}
		if got, want := info["persist_to_config"], true; got != want {
			t.Fatalf("persist_to_config: got %v, want %v", got, want)
		}
		if got, want := info["databases"], float64(2); got != want {
			t.Fatalf("databases: got %v, want %v", got, want)
		}
		if got, want := info["open_databases"], float64(1); got != want {
			t.Fatalf("open_databases: got %v, want %v", got, want)
		}
		if got, want := info["version"], Version; got != want {
			t.Fatalf("version: got %v, want %v", got, want)
		}
		if startedAt, ok := info["started_at"].(string); ok {
			if _, err := time.Parse(time.RFC3339, startedAt); err != nil {
				t.Fatalf("started_at parse: %v", err)
			}
		} else {
			t.Fatalf("started_at missing or not string")
		}
	})
}

func TestControlServer_handleRequest_PathExpansion(t *testing.T) {
	tmpDir := t.TempDir()

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldWd)
	})

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd after chdir: %v", err)
	}
	dbPath := filepath.Join(wd, "test.db")
	db := litestream.NewDB(dbPath)
	db.MonitorInterval = 0
	if err := os.MkdirAll(db.MetaPath(), 0o755); err != nil {
		t.Fatalf("create meta path: %v", err)
	}
	if err := db.Open(); err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })

	store := litestream.NewStore(nil, nil)
	if err := store.RegisterDB(db); err != nil {
		t.Fatalf("register db: %v", err)
	}

	controlServer := NewControlServer(store, nil, "", "", 0, nil)

	relPath := filepath.Base(dbPath)

	statusParams, err := json.Marshal(StatusParams{Path: relPath})
	if err != nil {
		t.Fatalf("marshal status params: %v", err)
	}
	statusReq := RPCRequest{
		JSONRPC: "2.0",
		Method:  "status",
		Params:  statusParams,
		ID:      1,
	}
	statusResp := controlServer.handleRequest(&statusReq)
	if statusResp.Error != nil {
		t.Fatalf("status error: %v", statusResp.Error.Message)
	}
	statusResult, ok := statusResp.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected status result map, got %T", statusResp.Result)
	}
	if got, want := statusResult["status"], "open"; got != want {
		t.Fatalf("status: got %v, want %v", got, want)
	}

	syncParams, err := json.Marshal(SyncParams{Path: relPath, Timeout: 1})
	if err != nil {
		t.Fatalf("marshal sync params: %v", err)
	}
	syncReq := RPCRequest{
		JSONRPC: "2.0",
		Method:  "sync",
		Params:  syncParams,
		ID:      2,
	}
	syncResp := controlServer.handleRequest(&syncReq)
	if syncResp.Error == nil || syncResp.Error.Message != "database has no replica" {
		t.Fatalf("expected missing replica error, got %#v", syncResp.Error)
	}

	stopParams, err := json.Marshal(StopParams{Path: relPath, Timeout: 1})
	if err != nil {
		t.Fatalf("marshal stop params: %v", err)
	}
	stopReq := RPCRequest{
		JSONRPC: "2.0",
		Method:  "stop",
		Params:  stopParams,
		ID:      3,
	}
	stopResp := controlServer.handleRequest(&stopReq)
	if stopResp.Error != nil {
		t.Fatalf("stop error: %v", stopResp.Error.Message)
	}
	if db.IsOpen() {
		t.Fatalf("expected db to be stopped")
	}
}
