package webdav_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/litestream/webdav"
)

func TestReplicaClient_Type(t *testing.T) {
	c := webdav.NewReplicaClient()
	if got, want := c.Type(), "webdav"; got != want {
		t.Fatalf("Type()=%v, want %v", got, want)
	}
}

func TestReplicaClient_Init_RequiresURL(t *testing.T) {
	c := webdav.NewReplicaClient()
	c.URL = ""

	if _, err := c.Init(context.TODO()); err == nil {
		t.Fatal("expected error when URL is empty")
	} else if got, want := err.Error(), "webdav url required"; got != want {
		t.Fatalf("error=%v, want %v", got, want)
	}
}
