package storage

import (
	"time"

	"github.com/forgekv/forgekv/internal/config"
	"github.com/forgekv/forgekv/internal/observability"
)

// BackpressureResult indicates the result of a backpressure check.
type BackpressureResult int

const (
	// BackpressureNone means no backpressure is needed.
	BackpressureNone BackpressureResult = iota
	// BackpressureDelay means a delay should be applied before proceeding.
	BackpressureDelay
	// BackpressureReject means the request should be rejected.
	BackpressureReject
)

// BackpressureController manages admission control based on Pebble health.
type BackpressureController struct {
	storage *Storage
	cfg     *config.Config
}

// NewBackpressureController creates a new backpressure controller.
func NewBackpressureController(storage *Storage, cfg *config.Config) *BackpressureController {
	return &BackpressureController{
		storage: storage,
		cfg:     cfg,
	}
}

// Check evaluates current storage health and returns backpressure decision.
// Returns the result and the delay to apply (only for BackpressureDelay).
func (bc *BackpressureController) Check() (BackpressureResult, time.Duration) {
	if bc.cfg.DisableBackpressure {
		return BackpressureNone, 0
	}

	l0Files, l0Bytes, writeStalled := bc.storage.GetHealth()

	// Hard limit: reject immediately
	if l0Files >= bc.cfg.L0HardFiles || l0Bytes >= bc.cfg.L0HardBytes || writeStalled {
		observability.RecordBackpressureReject()
		return BackpressureReject, 0
	}

	// Soft limit: apply delay
	if l0Files >= bc.cfg.L0SoftFiles || l0Bytes >= bc.cfg.L0SoftBytes {
		// delay_ms = min(25ms, 2ms * (L0Files - L0SoftFiles))
		delayMs := 2 * (l0Files - bc.cfg.L0SoftFiles)
		if delayMs > 25 {
			delayMs = 25
		}
		if delayMs < 0 {
			delayMs = 0
		}
		delay := time.Duration(delayMs) * time.Millisecond
		observability.RecordBackpressureDelay(float64(delayMs))
		return BackpressureDelay, delay
	}

	return BackpressureNone, 0
}

// ApplyBackpressure applies backpressure if needed.
// Returns true if the request should proceed, false if rejected.
func (bc *BackpressureController) ApplyBackpressure() bool {
	result, delay := bc.Check()
	switch result {
	case BackpressureReject:
		return false
	case BackpressureDelay:
		time.Sleep(delay)
		return true
	default:
		return true
	}
}
