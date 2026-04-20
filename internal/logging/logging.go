// Package logging owns the slog setup for the binary.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// LevelVar is the process-wide log level, exposed so the CLI can adjust it at runtime.
var LevelVar = new(slog.LevelVar)

// New returns a JSON slog.Logger writing to w at the given level name
// ("debug", "info", "warn", "error"; case-insensitive; unknown → info).
// Passing nil for w uses os.Stderr.
func New(w io.Writer, level string) *slog.Logger {
	if w == nil {
		w = os.Stderr
	}
	LevelVar.Set(parseLevel(level))
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:     LevelVar,
		AddSource: false,
	})
	return slog.New(h)
}

// InstallDefault installs lg as slog's default logger.
func InstallDefault(lg *slog.Logger) {
	slog.SetDefault(lg)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
