package main

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
