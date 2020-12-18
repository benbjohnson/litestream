package litestream

type Replicator interface {
}

// FileReplicator is a replicator that replicates a DB to a local file path.
type FileReplicator struct {
	db  *DB    // source database
	dst string // destination path
}

// NewFileReplicator returns a new instance of FileReplicator.
func NewFileReplicator(db *DB, dst string) *FileReplicator {
	return &FileReplicator{
		db:  db,
		dst: dst,
	}
}
