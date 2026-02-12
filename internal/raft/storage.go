// Package raft implements the Raft consensus layer for ForgeKV.
package raft

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Key prefixes for Raft storage
var (
	keyHardState    = []byte("raft_hard_state")
	keyConfState    = []byte("raft_conf_state")
	keySnapshotMeta = []byte("raft_snapshot_meta")
	prefixEntry     = []byte("raft_entry_")
)

// PebbleStorage implements raft.Storage using Pebble.
type PebbleStorage struct {
	mu       sync.RWMutex
	db       *pebble.DB
	path     string
	snapshot raftpb.Snapshot
}

// NewPebbleStorage creates a new Raft storage backed by Pebble.
func NewPebbleStorage(path string) (*PebbleStorage, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	db, err := pebble.Open(filepath.Join(path, "raft"), &pebble.Options{})
	if err != nil {
		return nil, err
	}

	ps := &PebbleStorage{
		db:   db,
		path: path,
	}

	// Load snapshot metadata if exists
	if err := ps.loadSnapshotMeta(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return ps, nil
}

// InitialState returns the saved HardState and ConfState information.
func (ps *PebbleStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	hs := raftpb.HardState{}
	cs := raftpb.ConfState{}

	// Load HardState
	data, closer, err := ps.db.Get(keyHardState)
	if err == nil {
		if closer != nil {
			defer func() { _ = closer.Close() }()
		}
		if err := hs.Unmarshal(data); err != nil {
			return hs, cs, err
		}
	} else if err != pebble.ErrNotFound {
		return hs, cs, err
	}

	// Load ConfState
	data, closer, err = ps.db.Get(keyConfState)
	if err == nil {
		if closer != nil {
			defer func() { _ = closer.Close() }()
		}
		if err := cs.Unmarshal(data); err != nil {
			return hs, cs, err
		}
	} else if err != pebble.ErrNotFound {
		return hs, cs, err
	}

	return hs, cs, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
func (ps *PebbleStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if lo <= ps.snapshot.Metadata.Index {
		return nil, raft.ErrCompacted
	}

	first, err := ps.firstIndex()
	if err != nil {
		return nil, err
	}
	if lo < first {
		return nil, raft.ErrCompacted
	}

	last, err := ps.lastIndex()
	if err != nil {
		return nil, err
	}
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}

	entries := make([]raftpb.Entry, 0, hi-lo)
	var size uint64

	for i := lo; i < hi; i++ {
		key := entryKey(i)
		data, closer, err := ps.db.Get(key)
		if err == pebble.ErrNotFound {
			break
		}
		if err != nil {
			return nil, err
		}

		var entry raftpb.Entry
		if err := entry.Unmarshal(data); err != nil {
			_ = closer.Close()
			return nil, err
		}
		_ = closer.Close()

		size += uint64(entry.Size())
		if len(entries) > 0 && size > maxSize {
			break
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()].
func (ps *PebbleStorage) Term(i uint64) (uint64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if i == ps.snapshot.Metadata.Index {
		return ps.snapshot.Metadata.Term, nil
	}

	if i < ps.snapshot.Metadata.Index {
		return 0, raft.ErrCompacted
	}

	first, err := ps.firstIndex()
	if err != nil {
		return 0, err
	}
	if i < first {
		return 0, raft.ErrCompacted
	}

	last, err := ps.lastIndex()
	if err != nil {
		return 0, err
	}
	if i > last {
		return 0, raft.ErrUnavailable
	}

	key := entryKey(i)
	data, closer, err := ps.db.Get(key)
	if err == pebble.ErrNotFound {
		return 0, raft.ErrUnavailable
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = closer.Close() }()

	var entry raftpb.Entry
	if err := entry.Unmarshal(data); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (ps *PebbleStorage) LastIndex() (uint64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.lastIndex()
}

func (ps *PebbleStorage) lastIndex() (uint64, error) {
	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixEntry,
		UpperBound: append(prefixEntry, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		// No entries, return snapshot index
		return ps.snapshot.Metadata.Index, nil
	}

	return indexFromKey(iter.Key()), nil
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries.
func (ps *PebbleStorage) FirstIndex() (uint64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.firstIndex()
}

func (ps *PebbleStorage) firstIndex() (uint64, error) {
	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixEntry,
		UpperBound: append(prefixEntry, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	if !iter.First() {
		// No entries, return snapshot index + 1
		return ps.snapshot.Metadata.Index + 1, nil
	}

	return indexFromKey(iter.Key()), nil
}

// Snapshot returns the most recent snapshot.
func (ps *PebbleStorage) Snapshot() (raftpb.Snapshot, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.snapshot, nil
}

// SaveHardState saves the current HardState.
func (ps *PebbleStorage) SaveHardState(hs raftpb.HardState) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := hs.Marshal()
	if err != nil {
		return err
	}
	return ps.db.Set(keyHardState, data, pebble.Sync)
}

// SaveConfState saves the current ConfState.
func (ps *PebbleStorage) SaveConfState(cs raftpb.ConfState) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := cs.Marshal()
	if err != nil {
		return err
	}
	return ps.db.Set(keyConfState, data, pebble.Sync)
}

// Append appends entries to the log.
func (ps *PebbleStorage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	batch := ps.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, entry := range entries {
		data, err := entry.Marshal()
		if err != nil {
			return err
		}
		key := entryKey(entry.Index)
		if err := batch.Set(key, data, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// ApplySnapshot applies a snapshot.
func (ps *PebbleStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if snap.Metadata.Index <= ps.snapshot.Metadata.Index {
		return nil
	}

	ps.snapshot = snap

	// Save snapshot metadata
	data, err := snap.Metadata.Marshal()
	if err != nil {
		return err
	}
	if err := ps.db.Set(keySnapshotMeta, data, pebble.Sync); err != nil {
		return err
	}

	// Delete entries before snapshot
	batch := ps.db.NewBatch()
	defer func() { _ = batch.Close() }()

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixEntry,
		UpperBound: entryKey(snap.Metadata.Index + 1),
	})
	if err != nil {
		return err
	}

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			_ = iter.Close()
			return err
		}
	}
	_ = iter.Close()

	return batch.Commit(pebble.Sync)
}

// CreateSnapshot creates a snapshot with the given index, term, and confstate.
func (ps *PebbleStorage) CreateSnapshot(index, term uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if index <= ps.snapshot.Metadata.Index {
		return raftpb.Snapshot{}, errors.New("snapshot index too small")
	}

	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: *cs,
		},
		Data: data,
	}

	ps.snapshot = snap

	// Save snapshot metadata
	metaData, err := snap.Metadata.Marshal()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	if err := ps.db.Set(keySnapshotMeta, metaData, pebble.Sync); err != nil {
		return raftpb.Snapshot{}, err
	}

	// Compact old entries
	batch := ps.db.NewBatch()
	defer func() { _ = batch.Close() }()

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixEntry,
		UpperBound: entryKey(index),
	})
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			_ = iter.Close()
			return raftpb.Snapshot{}, err
		}
	}
	_ = iter.Close()

	if err := batch.Commit(pebble.Sync); err != nil {
		return raftpb.Snapshot{}, err
	}

	return snap, nil
}

func (ps *PebbleStorage) loadSnapshotMeta() error {
	data, closer, err := ps.db.Get(keySnapshotMeta)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()

	var meta raftpb.SnapshotMetadata
	if err := meta.Unmarshal(data); err != nil {
		return err
	}
	ps.snapshot.Metadata = meta
	return nil
}

// Close closes the storage.
func (ps *PebbleStorage) Close() error {
	return ps.db.Close()
}

// entryKey creates a key for a log entry.
func entryKey(index uint64) []byte {
	key := make([]byte, len(prefixEntry)+8)
	copy(key, prefixEntry)
	binary.BigEndian.PutUint64(key[len(prefixEntry):], index)
	return key
}

// indexFromKey extracts the index from an entry key.
func indexFromKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(prefixEntry):])
}
