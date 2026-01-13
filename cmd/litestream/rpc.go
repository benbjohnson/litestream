package main

import (
	"encoding/json"
)

// RPCRequest represents a JSON-RPC 2.0 request.
type RPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      interface{}     `json:"id"`
}

// RPCResponse represents a JSON-RPC 2.0 response.
type RPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

// RPCError represents a JSON-RPC error.
type RPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// newSuccessResponse creates a successful JSON-RPC response.
func newSuccessResponse(id interface{}, result interface{}) *RPCResponse {
	return &RPCResponse{
		JSONRPC: "2.0",
		Result:  result,
		ID:      id,
	}
}

// newErrorResponse creates an error JSON-RPC response.
func newErrorResponse(id interface{}, code int, message string, data interface{}) *RPCResponse {
	return &RPCResponse{
		JSONRPC: "2.0",
		Error: &RPCError{
			Code:    code,
			Message: message,
			Data:    data,
		},
		ID: id,
	}
}

// StartParams contains parameters for the start command.
type StartParams struct {
	Path    string `json:"path"`
	Config  string `json:"config,omitempty"`
	Wait    bool   `json:"wait"`
	Timeout int    `json:"timeout"`
}

// StopParams contains parameters for the stop command.
type StopParams struct {
	Path    string `json:"path"`
	Wait    bool   `json:"wait"`
	Timeout int    `json:"timeout"`
}

// SyncParams contains parameters for the sync command.
type SyncParams struct {
	Path    string `json:"path"`
	Wait    bool   `json:"wait"`
	Timeout int    `json:"timeout"`
}

// StatusParams contains parameters for the status command.
type StatusParams struct {
	Path string `json:"path"`
}

// DatabasesParams contains parameters for the databases command.
type DatabasesParams struct {
	// No parameters for databases command
}
