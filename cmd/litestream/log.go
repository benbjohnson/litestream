package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"

	"github.com/benbjohnson/litestream/internal"
)

// LoggingConfig configures logging.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Type   string `yaml:"type"`
	Stderr bool   `yaml:"stderr"`
	Source bool   `yaml:"source"`
}

func initLog(w io.Writer, level, typ string, addSource bool) {
	logOptions := slog.HandlerOptions{
		Level:       slog.LevelInfo,
		AddSource:   addSource,
		ReplaceAttr: internal.ReplaceAttr,
	}

	// Read log level from environment, if available.
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		level = v
	}

	switch strings.ToUpper(level) {
	case "TRACE":
		logOptions.Level = internal.LevelTrace
	case "DEBUG":
		logOptions.Level = slog.LevelDebug
	case "INFO":
		logOptions.Level = slog.LevelInfo
	case "WARN", "WARNING":
		logOptions.Level = slog.LevelWarn
	case "ERROR":
		logOptions.Level = slog.LevelError
	}

	var logHandler slog.Handler
	switch typ {
	case "json":
		logHandler = slog.NewJSONHandler(w, &logOptions)
	case "pretty":
		noColor := true
		if f, ok := w.(*os.File); ok {
			noColor = !isatty.IsTerminal(f.Fd()) && !isatty.IsCygwinTerminal(f.Fd())
		}
		logHandler = tint.NewHandler(w, &tint.Options{
			Level:       logOptions.Level,
			AddSource:   addSource,
			TimeFormat:  time.TimeOnly,
			NoColor:     noColor,
			ReplaceAttr: internal.ReplaceAttr,
		})
	case "text", "":
		logHandler = slog.NewTextHandler(w, &logOptions)
	}

	// Set global default logger.
	slog.SetDefault(slog.New(logHandler))
}
