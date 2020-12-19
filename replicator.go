package litestream

import (
	"context"
)

// Replicator represents a method for replicating the snapshot & WAL data to
// a remote destination.
type Replicator interface {
	Name() string
	Type() string
	BeginSnapshot(ctx context.Context) error
}

var _ Replicator = (*FileReplicator)(nil)

// FileReplicator is a replicator that replicates a DB to a local file path.
type FileReplicator struct {
	db   *DB    // source database
	name string // replicator name, optional
	dst  string // destination path
}

// NewFileReplicator returns a new instance of FileReplicator.
func NewFileReplicator(db *DB, name, dst string) *FileReplicator {
	return &FileReplicator{
		db:   db,
		name: name,
		dst:  dst,
	}
}

// Name returns the name of the replicator. Returns the type if no name set.
func (r *FileReplicator) Name() string {
	if r.name != "" {
		return r.name
	}
	return r.Type()
}

// Type returns the type of replicator.
func (r *FileReplicator) Type() string {
	return "file"
}

//
func (r *FileReplicator) BeginSnapshot(ctx context.Context) error {
	// TODO: Set snapshotting state to true.
	// TODO: Read current generation.
	// TODO: Copy database to destination.
	return nil
}
