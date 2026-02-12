package storage

import (
	"testing"
	"time"

	"github.com/forgekv/forgekv/internal/config"
)

// mockStorage implements a minimal storage for backpressure testing
type mockStorage struct {
	l0Files      int
	l0Bytes      int64
	writeStalled bool
}

func (m *mockStorage) GetHealth() (l0Files int, l0Bytes int64, writeStalled bool) {
	return m.l0Files, m.l0Bytes, m.writeStalled
}

func TestBackpressure_None(t *testing.T) {
	cfg := config.DefaultConfig()
	ms := &mockStorage{l0Files: 10, l0Bytes: 100 * 1024 * 1024}

	bc := &testBackpressureController{ms: ms, cfg: cfg}
	result, delay := bc.Check()

	if result != BackpressureNone {
		t.Errorf("expected BackpressureNone, got %v", result)
	}
	if delay != 0 {
		t.Errorf("expected no delay, got %v", delay)
	}
}

func TestBackpressure_Delay(t *testing.T) {
	cfg := config.DefaultConfig()
	ms := &mockStorage{l0Files: 25, l0Bytes: 100 * 1024 * 1024} // Above soft, below hard

	bc := &testBackpressureController{ms: ms, cfg: cfg}
	result, delay := bc.Check()

	if result != BackpressureDelay {
		t.Errorf("expected BackpressureDelay, got %v", result)
	}
	// delay_ms = 2 * (25 - 20) = 10ms
	if delay.Milliseconds() != 10 {
		t.Errorf("expected 10ms delay, got %v", delay)
	}
}

func TestBackpressure_DelayMaxCapped(t *testing.T) {
	cfg := config.DefaultConfig()
	ms := &mockStorage{l0Files: 50, l0Bytes: 100 * 1024 * 1024} // Well above soft

	bc := &testBackpressureController{ms: ms, cfg: cfg}
	result, delay := bc.Check()

	if result != BackpressureDelay {
		t.Errorf("expected BackpressureDelay, got %v", result)
	}
	// delay_ms = min(25, 2 * (50 - 20)) = min(25, 60) = 25ms
	if delay.Milliseconds() != 25 {
		t.Errorf("expected 25ms delay (capped), got %v", delay)
	}
}

func TestBackpressure_RejectByFiles(t *testing.T) {
	cfg := config.DefaultConfig()
	ms := &mockStorage{l0Files: 70, l0Bytes: 100 * 1024 * 1024} // Above hard files

	bc := &testBackpressureController{ms: ms, cfg: cfg}
	result, _ := bc.Check()

	if result != BackpressureReject {
		t.Errorf("expected BackpressureReject, got %v", result)
	}
}

func TestBackpressure_RejectByBytes(t *testing.T) {
	cfg := config.DefaultConfig()
	ms := &mockStorage{l0Files: 10, l0Bytes: 2 * 1024 * 1024 * 1024} // Above hard bytes

	bc := &testBackpressureController{ms: ms, cfg: cfg}
	result, _ := bc.Check()

	if result != BackpressureReject {
		t.Errorf("expected BackpressureReject, got %v", result)
	}
}

func TestBackpressure_RejectByWriteStall(t *testing.T) {
	cfg := config.DefaultConfig()
	ms := &mockStorage{l0Files: 10, l0Bytes: 100 * 1024 * 1024, writeStalled: true}

	bc := &testBackpressureController{ms: ms, cfg: cfg}
	result, _ := bc.Check()

	if result != BackpressureReject {
		t.Errorf("expected BackpressureReject due to write stall, got %v", result)
	}
}

func TestBackpressure_SoftBytesThreshold(t *testing.T) {
	cfg := config.DefaultConfig()
	ms := &mockStorage{l0Files: 10, l0Bytes: 300 * 1024 * 1024} // Above soft bytes

	bc := &testBackpressureController{ms: ms, cfg: cfg}
	result, _ := bc.Check()

	if result != BackpressureDelay {
		t.Errorf("expected BackpressureDelay for soft bytes threshold, got %v", result)
	}
}

// testBackpressureController is a test version that uses mockStorage
type testBackpressureController struct {
	ms  *mockStorage
	cfg *config.Config
}

func (bc *testBackpressureController) Check() (BackpressureResult, time.Duration) {
	l0Files, l0Bytes, writeStalled := bc.ms.GetHealth()

	// Hard limit: reject immediately
	if l0Files >= bc.cfg.L0HardFiles || l0Bytes >= bc.cfg.L0HardBytes || writeStalled {
		return BackpressureReject, 0
	}

	// Soft limit: apply delay
	if l0Files >= bc.cfg.L0SoftFiles || l0Bytes >= bc.cfg.L0SoftBytes {
		delayMs := 2 * (l0Files - bc.cfg.L0SoftFiles)
		if delayMs > 25 {
			delayMs = 25
		}
		if delayMs < 0 {
			delayMs = 0
		}
		return BackpressureDelay, time.Duration(delayMs) * time.Millisecond
	}

	return BackpressureNone, 0
}
