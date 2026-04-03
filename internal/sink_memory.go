package internal

import (
	"context"
	"sync"
)

// MemorySink stores audit events in memory for testing.
type MemorySink struct {
	mu     sync.Mutex
	events []AuditEvent
}

func NewMemorySink() *MemorySink { return &MemorySink{} }

func (m *MemorySink) Write(_ context.Context, events []AuditEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, events...)
	return nil
}

func (m *MemorySink) Query(_ context.Context, q AuditQuery) ([]AuditEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []AuditEvent
	for _, ev := range m.events {
		if matchesQuery(ev, q) {
			result = append(result, ev)
			if q.Limit > 0 && len(result) >= q.Limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MemorySink) Close() error { return nil }

// Events returns a copy of all stored events.
func (m *MemorySink) Events() []AuditEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]AuditEvent(nil), m.events...)
}
