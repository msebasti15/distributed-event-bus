// Package logging provides the small, level-aware logger shared by the demos.
package logging

import (
	"io"
	"log"
	"strings"
)

// Level controls which messages are emitted.
type Level int

const (
	LevelError Level = 0
	LevelWarn  Level = 1
	LevelInfo  Level = 2
	LevelDebug Level = 3
)

// ParseLevel converts a command-line value into a logging level.
func ParseLevel(value string) (Level, bool) {
	switch strings.ToLower(value) {
	case "error":
		return LevelError, true
	case "warn", "warning":
		return LevelWarn, true
	case "info":
		return LevelInfo, true
	case "debug":
		return LevelDebug, true
	default:
		return LevelInfo, false
	}
}

// Logger emits messages at or above the configured level.
type Logger struct {
	level Level
	log   *log.Logger
}

// New creates a logger writing to out.
func New(level Level, out io.Writer) *Logger {
	return &Logger{level: level, log: log.New(out, "", log.LstdFlags|log.Lmicroseconds)}
}

func (l *Logger) Errorf(format string, args ...any) { l.logf(LevelError, "ERROR", format, args...) }
func (l *Logger) Warnf(format string, args ...any)  { l.logf(LevelWarn, "WARN", format, args...) }
func (l *Logger) Infof(format string, args ...any)  { l.logf(LevelInfo, "INFO", format, args...) }
func (l *Logger) Debugf(format string, args ...any) { l.logf(LevelDebug, "DEBUG", format, args...) }

func (l *Logger) logf(level Level, name, format string, args ...any) {
	if l == nil || level > l.level {
		return
	}
	l.log.Printf("[%s] "+format, append([]any{name}, args...)...)
}
