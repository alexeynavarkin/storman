// Package logging sets up the default slog logger from environment variables.
// Called once from cmd/storman/main.go so every subcommand inherits the same
// handler. Stdlib log.Printf calls are automatically routed through slog at
// LevelInfo by slog.SetDefault (Go 1.21+).
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Setup configures slog.Default from STORMAN_LOG_FORMAT (text|json, default
// text) and STORMAN_LOG_LEVEL (debug|info|warn|error, default info).
//
// Writes to stderr — stdout is reserved for tool-readable output like
// `storman version`.
func Setup() {
	level := parseLevel(os.Getenv("STORMAN_LOG_LEVEL"))
	format := strings.ToLower(strings.TrimSpace(os.Getenv("STORMAN_LOG_FORMAT")))

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	default:
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
