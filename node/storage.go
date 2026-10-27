package node

import "sync"

// Storage persists the node's state across deep sleep and power loss: its
// identity, request counter, pairing, route to the hub, config and pending
// events. Each key holds a small blob (well under a kilobyte, except the
// config). Implementations back it with flash, RTC memory or a file.
type Storage interface {
	// Load returns the data stored under key, or nil if there is none.
	Load(key string) ([]byte, error)
	// Save stores data under key, replacing what was there.
	Save(key string, data []byte) error
}

// MemStorage is a Storage in RAM. It keeps state across Sync calls but not
// across power loss, so it suits nodes that never sleep, and tests.
type MemStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (s *MemStorage) Load(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

func (s *MemStorage) Save(key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string][]byte{}
	}
	s.m[key] = append([]byte(nil), data...)
	return nil
}
