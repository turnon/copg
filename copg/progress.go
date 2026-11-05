package copg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
)

// ProgressStore persists which shards have been exported successfully. It
// enables resuming an interrupted export: shards reported as completed are
// skipped on the next run.
type ProgressStore interface {
	// IsCompleted reports whether shardID was fully exported.
	IsCompleted(ctx context.Context, shardID string) (bool, error)
	// MarkCompleted records shardID as fully exported.
	MarkCompleted(ctx context.Context, shardID string) error
}

// FileProgressStore is a ProgressStore that appends completed shard IDs to a
// text file, one ID per line. Existing progress is loaded from the file on
// startup.
type FileProgressStore struct {
	path string
	mu   sync.Mutex
	done map[string]bool
}

// NewFileProgressStore opens (or creates) path and loads already completed
// shard IDs.
func NewFileProgressStore(path string) (*FileProgressStore, error) {
	if path == "" {
		return nil, fmt.Errorf("copg: progress file path is empty")
	}
	store := &FileProgressStore{path: path, done: make(map[string]bool)}

	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("copg: read progress file: %w", err)
		}
		return store, nil
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			store.done[line] = true
		}
	}
	return store, nil
}

// IsCompleted implements ProgressStore.
func (s *FileProgressStore) IsCompleted(_ context.Context, shardID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done[shardID], nil
}

// MarkCompleted implements ProgressStore. It appends to the progress file, so a
// crash can at worst leave a partial line; a partially written ID is simply not
// treated as completed on the next run.
func (s *FileProgressStore) MarkCompleted(_ context.Context, shardID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done[shardID] {
		return nil
	}

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("copg: open progress file: %w", err)
	}
	defer f.Close()

	if _, err := fmt.Fprintln(f, shardID); err != nil {
		return fmt.Errorf("copg: write progress file: %w", err)
	}
	// Best effort durability; failure to sync is reported so the caller does
	// not silently lose resumability.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("copg: sync progress file: %w", err)
	}

	s.done[shardID] = true
	return nil
}

// MemoryProgressStore is a non-persistent ProgressStore, mostly useful for
// tests and single-run pipelines.
type MemoryProgressStore struct {
	mu   sync.Mutex
	done map[string]bool
}

// NewMemoryProgressStore returns an empty in-memory progress store.
func NewMemoryProgressStore() *MemoryProgressStore {
	return &MemoryProgressStore{done: make(map[string]bool)}
}

// IsCompleted implements ProgressStore.
func (s *MemoryProgressStore) IsCompleted(_ context.Context, shardID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done[shardID], nil
}

// MarkCompleted implements ProgressStore.
func (s *MemoryProgressStore) MarkCompleted(_ context.Context, shardID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done[shardID] = true
	return nil
}
