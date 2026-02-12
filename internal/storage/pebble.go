// Package storage provides the Pebble-based storage engine for ForgeKV.
package storage

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/forgekv/forgekv/internal/observability"
	"go.uber.org/zap"
)

// Key prefixes for different data types
const (
	PrefixUserKey   byte = 0x01
	PrefixSession   byte = 0x02
	PrefixMetadata  byte = 0x03
)

// Storage wraps Pebble with ForgeKV-specific functionality.
type Storage struct {
	db     *pebble.DB
	opts   *pebble.Options
	path   string
	logger *zap.Logger

	// Metrics sampling
	mu           sync.RWMutex
	l0Files      int
	l0Bytes      int64
	writeStalled atomic.Bool

	// Injected filesystem for fault injection
	fs vfs.FS

	// Disk stall injection
	fsyncDelay atomic.Int64 // in milliseconds
}

// StallableFS wraps a filesystem to inject fsync delays.
type StallableFS struct {
	vfs.FS
	storage *Storage
}

// StallableFile wraps a file to inject fsync delays.
type StallableFile struct {
	vfs.File
	storage *Storage
}

func (f *StallableFile) Sync() error {
	delay := f.storage.fsyncDelay.Load()
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	return f.File.Sync()
}

func (fs *StallableFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &StallableFile{File: f, storage: fs.storage}, nil
}

func (fs *StallableFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &StallableFile{File: f, storage: fs.storage}, nil
}

func (fs *StallableFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &StallableFile{File: f, storage: fs.storage}, nil
}

func (fs *StallableFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err != nil {
		return nil, err
	}
	return &StallableFile{File: f, storage: fs.storage}, nil
}

// NewStorage creates a new Pebble-based storage engine.
// memTableSize overrides Pebble's default MemTableSize if > 0.
func NewStorage(path string, logger *zap.Logger, memTableSize int) (*Storage, error) {
	s := &Storage{
		path:   path,
		logger: logger.Named("storage"),
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	// Use stallable filesystem wrapper
	baseFS := vfs.Default
	s.fs = &StallableFS{FS: baseFS, storage: s}

	opts := &pebble.Options{
		FS: s.fs,
		// Comparer uses default byte comparison
		// EventListener for metrics
		EventListener: &pebble.EventListener{
			WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
				s.writeStalled.Store(true)
				s.logger.Warn("pebble write stall begin", zap.String("reason", info.Reason))
				observability.PebbleWriteStallTotal.Inc()
			},
			WriteStallEnd: func() {
				s.writeStalled.Store(false)
				s.logger.Info("pebble write stall end")
			},
		},
	}

	if memTableSize > 0 {
		opts.MemTableSize = uint64(memTableSize)
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	s.db = db
	s.opts = opts

	// Start metrics sampler
	go s.sampleMetrics()

	return s, nil
}

// sampleMetrics periodically samples Pebble health metrics.
func (s *Storage) sampleMetrics() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if s.db == nil {
			return
		}
		metrics := s.db.Metrics()
		s.mu.Lock()
		s.l0Files = int(metrics.Levels[0].NumFiles)
		s.l0Bytes = metrics.Levels[0].Size
		s.mu.Unlock()

		observability.UpdatePebbleMetrics(s.l0Files, s.l0Bytes)
	}
}

// GetHealth returns current storage health signals for backpressure decisions.
func (s *Storage) GetHealth() (l0Files int, l0Bytes int64, writeStalled bool) {
	s.mu.RLock()
	l0Files = s.l0Files
	l0Bytes = s.l0Bytes
	s.mu.RUnlock()
	writeStalled = s.writeStalled.Load()
	return
}

// SetFsyncDelay sets the artificial fsync delay for fault injection.
func (s *Storage) SetFsyncDelay(delayMs uint32) {
	s.fsyncDelay.Store(int64(delayMs))
	s.logger.Info("fsync delay set", zap.Uint32("delay_ms", delayMs))
}

// Get retrieves a value by key (without prefix).
func (s *Storage) Get(key []byte) ([]byte, bool, error) {
	prefixedKey := append([]byte{PrefixUserKey}, key...)
	value, closer, err := s.db.Get(prefixedKey)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = closer.Close() }()

	// Copy the value since it's only valid until closer.Close()
	result := make([]byte, len(value))
	copy(result, value)
	return result, true, nil
}

// Put stores a key-value pair (without prefix).
func (s *Storage) Put(key, value []byte) error {
	prefixedKey := append([]byte{PrefixUserKey}, key...)
	return s.db.Set(prefixedKey, value, pebble.Sync)
}

// Delete removes a key (without prefix).
func (s *Storage) Delete(key []byte) error {
	prefixedKey := append([]byte{PrefixUserKey}, key...)
	return s.db.Delete(prefixedKey, pebble.Sync)
}

// GetSession retrieves a session record.
func (s *Storage) GetSession(clientID string) ([]byte, bool, error) {
	key := append([]byte{PrefixSession}, []byte(clientID)...)
	value, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = closer.Close() }()

	result := make([]byte, len(value))
	copy(result, value)
	return result, true, nil
}

// PutSession stores a session record.
func (s *Storage) PutSession(clientID string, data []byte) error {
	key := append([]byte{PrefixSession}, []byte(clientID)...)
	return s.db.Set(key, data, pebble.Sync)
}

// DeleteSession removes a session record.
func (s *Storage) DeleteSession(clientID string) error {
	key := append([]byte{PrefixSession}, []byte(clientID)...)
	return s.db.Delete(key, pebble.Sync)
}

// Batch represents a write batch.
type Batch struct {
	batch *pebble.Batch
}

// NewBatch creates a new write batch.
func (s *Storage) NewBatch() *Batch {
	return &Batch{batch: s.db.NewBatch()}
}

// Put adds a put operation to the batch.
func (b *Batch) Put(key, value []byte) {
	prefixedKey := append([]byte{PrefixUserKey}, key...)
	_ = b.batch.Set(prefixedKey, value, nil)
}

// Delete adds a delete operation to the batch.
func (b *Batch) Delete(key []byte) {
	prefixedKey := append([]byte{PrefixUserKey}, key...)
	_ = b.batch.Delete(prefixedKey, nil)
}

// PutSession adds a session put to the batch.
func (b *Batch) PutSession(clientID string, data []byte) {
	key := append([]byte{PrefixSession}, []byte(clientID)...)
	_ = b.batch.Set(key, data, nil)
}

// DeleteSession adds a session delete to the batch.
func (b *Batch) DeleteSession(clientID string) {
	key := append([]byte{PrefixSession}, []byte(clientID)...)
	_ = b.batch.Delete(key, nil)
}

// Commit commits the batch with sync.
func (b *Batch) Commit() error {
	return b.batch.Commit(pebble.Sync)
}

// Close closes the batch without committing.
func (b *Batch) Close() error {
	return b.batch.Close()
}

// CreateCheckpoint creates a consistent checkpoint of the database.
func (s *Storage) CreateCheckpoint(destPath string) error {
	return s.db.Checkpoint(destPath)
}

// IterateSessions iterates over all session records.
func (s *Storage) IterateSessions(fn func(clientID string, data []byte) error) error {
	prefix := []byte{PrefixSession}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte{PrefixSession + 1},
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		clientID := string(key[1:]) // Skip prefix
		value := iter.Value()
		data := make([]byte, len(value))
		copy(data, value)
		if err := fn(clientID, data); err != nil {
			return err
		}
	}
	return iter.Error()
}

// Close closes the storage engine.
func (s *Storage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// DB returns the underlying Pebble database for advanced operations.
func (s *Storage) DB() *pebble.DB {
	return s.db
}
