package internal_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// mockSubscriber records Subscribe/Unsubscribe calls and allows delivering messages.
type mockSubscriber struct {
	mu       sync.Mutex
	handlers map[string]func([]byte, map[string]string) error
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{handlers: make(map[string]func([]byte, map[string]string) error)}
}

func (m *mockSubscriber) Subscribe(topic string, handler func([]byte, map[string]string) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[topic] = handler
	return nil
}

func (m *mockSubscriber) Unsubscribe(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handlers, topic)
	return nil
}

func (m *mockSubscriber) deliver(topic string, payload []byte, metadata map[string]string) error {
	m.mu.Lock()
	h, ok := m.handlers[topic]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return h(payload, metadata)
}

func (m *mockSubscriber) topicCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.handlers)
}

type mockPublisher struct{}

func (m *mockPublisher) Publish(topic string, payload []byte, metadata map[string]string) (string, error) {
	return "mock-id", nil
}

func TestNewPlugin_ImplementsProviders(t *testing.T) {
	p := internal.NewPlugin()
	var _ sdk.PluginProvider = p
	var _ sdk.ModuleProvider = p.(sdk.ModuleProvider)
	var _ sdk.StepProvider = p.(sdk.StepProvider)
}

func TestManifest_HasRequiredFields(t *testing.T) {
	m := internal.Manifest
	if m.Name != "workflow-plugin-audit" {
		t.Errorf("expected name 'workflow-plugin-audit', got %q", m.Name)
	}
	if m.Version == "" {
		t.Error("manifest Version is empty")
	}
}

func TestCollector_SubscribesAndBuffers(t *testing.T) {
	sub := newMockSubscriber()
	sink := internal.NewMemorySink()

	collector := internal.NewCollectorModule("test-collector", map[string]any{
		"topics":        []any{"test.topic"},
		"flushInterval": float64(1),
		"bufferSize":    float64(10),
	})
	collector.SetMessageSubscriber(sub)
	collector.SetMessagePublisher(&mockPublisher{})
	collector.RegisterSink(sink)

	if err := collector.Init(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := collector.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Deliver some events
	for i := 0; i < 5; i++ {
		payload, _ := json.Marshal(map[string]any{"index": i})
		if err := sub.deliver("test.topic", payload, map[string]string{
			"workflowId":    "wf-1",
			"actor":         "test-user",
			"correlationId": "corr-1",
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for flush
	time.Sleep(2 * time.Second)

	events := sink.Events()
	if len(events) != 5 {
		t.Errorf("expected 5 events, got %d", len(events))
	}

	// Verify event fields
	if len(events) > 0 {
		ev := events[0]
		if ev.EventType != "test.topic" {
			t.Errorf("expected eventType 'test.topic', got %q", ev.EventType)
		}
		if ev.WorkflowID != "wf-1" {
			t.Errorf("expected workflowId 'wf-1', got %q", ev.WorkflowID)
		}
		if ev.Actor != "test-user" {
			t.Errorf("expected actor 'test-user', got %q", ev.Actor)
		}
		if ev.ID == "" {
			t.Error("event ID should not be empty")
		}
	}

	// Stop and verify unsubscribe
	if err := collector.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if sub.topicCount() != 0 {
		t.Errorf("expected 0 subscriptions after stop, got %d", sub.topicCount())
	}
}

func TestCollector_FlushOnBufferFull(t *testing.T) {
	sub := newMockSubscriber()
	sink := internal.NewMemorySink()

	collector := internal.NewCollectorModule("test-collector", map[string]any{
		"topics":        []any{"test.topic"},
		"flushInterval": float64(1),
		"bufferSize":    float64(5),
	})
	collector.SetMessageSubscriber(sub)
	collector.RegisterSink(sink)

	if err := collector.Init(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := collector.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Deliver enough events to exceed buffer
	for i := 0; i < 10; i++ {
		payload := []byte(`{"test":true}`)
		_ = sub.deliver("test.topic", payload, map[string]string{})
	}

	// Wait for a flush cycle
	time.Sleep(2 * time.Second)

	events := sink.Events()
	if len(events) < 10 {
		t.Errorf("expected at least 10 events flushed, got %d", len(events))
	}

	_ = collector.Stop(ctx)
}

func TestCollector_StopDrainsRemaining(t *testing.T) {
	sub := newMockSubscriber()
	sink := internal.NewMemorySink()

	collector := internal.NewCollectorModule("test-collector", map[string]any{
		"topics":        []any{"test.topic"},
		"flushInterval": float64(300), // very long
		"bufferSize":    float64(1000),
	})
	collector.SetMessageSubscriber(sub)
	collector.RegisterSink(sink)

	_ = collector.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = collector.Start(ctx)

	// Deliver events
	for i := 0; i < 3; i++ {
		_ = sub.deliver("test.topic", []byte(`{}`), map[string]string{})
	}

	// Small delay to ensure events are in channel
	time.Sleep(50 * time.Millisecond)

	// Stop should drain
	_ = collector.Stop(ctx)

	events := sink.Events()
	if len(events) != 3 {
		t.Errorf("expected 3 events after stop drain, got %d", len(events))
	}
}
