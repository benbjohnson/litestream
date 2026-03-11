package litestream

import "log/slog"

const (
	LogKeySystem    = "system"
	LogKeySubsystem = "subsystem"
	LogKeyDB        = "db"
)

const (
	LogSystemStore  = "store"
	LogSystemServer = "server"
)

const (
	LogSubsystemCompactor = "compactor"
	LogSubsystemWALReader = "wal-reader"
)

// LoggerSetter is implemented by types that support logger propagation.
type LoggerSetter interface {
	SetLogger(logger *slog.Logger)
}

var _ LoggerSetter = (*DB)(nil)
