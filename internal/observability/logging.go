// Package observability provides structured logging, metrics, and tracing for ForgeKV.
package observability

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	loggerOnce   sync.Once
)

// InitLogger initializes the global structured JSON logger.
func InitLogger(nodeID string, debug bool) *zap.Logger {
	loggerOnce.Do(func() {
		config := zap.NewProductionEncoderConfig()
		config.TimeKey = "ts"
		config.LevelKey = "level"
		config.MessageKey = "msg"
		config.EncodeTime = zapcore.ISO8601TimeEncoder

		level := zapcore.InfoLevel
		if debug {
			level = zapcore.DebugLevel
		}

		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(config),
			zapcore.AddSync(os.Stdout),
			level,
		)

		globalLogger = zap.New(core).With(
			zap.String("node_id", nodeID),
		)
	})
	return globalLogger
}

// GetLogger returns the global logger. Must call InitLogger first.
func GetLogger() *zap.Logger {
	if globalLogger == nil {
		// Fallback to development logger
		globalLogger, _ = zap.NewDevelopment()
	}
	return globalLogger
}

// Logger returns a named child logger.
func Logger(name string) *zap.Logger {
	return GetLogger().Named(name)
}

// WithRaftState returns a logger with Raft state fields.
func WithRaftState(logger *zap.Logger, term uint64, isLeader bool) *zap.Logger {
	return logger.With(
		zap.Uint64("term", term),
		zap.Bool("is_leader", isLeader),
	)
}

// WithTraceID returns a logger with trace ID field.
func WithTraceID(logger *zap.Logger, traceID string) *zap.Logger {
	if traceID == "" {
		return logger
	}
	return logger.With(zap.String("trace_id", traceID))
}

// WithClientOp returns a logger with client operation fields.
func WithClientOp(logger *zap.Logger, clientID string, seq uint64, op string, keyLen int) *zap.Logger {
	return logger.With(
		zap.String("client_id", clientID),
		zap.Uint64("seq", seq),
		zap.String("op", op),
		zap.Int("key_len", keyLen),
	)
}

// WithError returns a logger with error fields.
func WithError(logger *zap.Logger, code string, err error) *zap.Logger {
	fields := []zap.Field{zap.String("code", code)}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	return logger.With(fields...)
}

// Sync flushes any buffered log entries.
func Sync() {
	if globalLogger != nil {
		_ = globalLogger.Sync()
	}
}
