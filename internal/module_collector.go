package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// CollectorModule subscribes to EventBus topics and dispatches events to sinks.
type CollectorModule struct {
	name   string
	config CollectorConfig
	sinks  []AuditSink

	sub sdk.MessageSubscriber
	pub sdk.MessagePublisher

	eventCh      chan AuditEvent
	stopCh       chan struct{}
	wg           sync.WaitGroup
	sinksMu      sync.RWMutex
	droppedCount atomic.Int64
}

// CollectorConfig holds configuration for the collector module.
type CollectorConfig struct {
	Topics        []string
	FlushInterval int // seconds
	BufferSize    int
}

func defaultCollectorConfig() CollectorConfig {
	return CollectorConfig{
		Topics:        []string{"workflow.*", "step.*", "pipeline.*"},
		FlushInterval: 5,
		BufferSize:    100,
	}
}

func parseCollectorConfig(raw map[string]any) CollectorConfig {
	cfg := defaultCollectorConfig()
	if raw == nil {
		return cfg
	}
	if v, ok := raw["topics"]; ok {
		switch t := v.(type) {
		case []any:
			topics := make([]string, 0, len(t))
			for _, item := range t {
				if s, ok := item.(string); ok {
					topics = append(topics, s)
				}
			}
			if len(topics) > 0 {
				cfg.Topics = topics
			}
		}
	}
	if v, ok := raw["flushInterval"]; ok {
		if f, ok := v.(float64); ok {
			cfg.FlushInterval = int(f)
		}
	}
	if v, ok := raw["bufferSize"]; ok {
		if f, ok := v.(float64); ok {
			cfg.BufferSize = int(f)
		}
	}
	return cfg
}

// NewCollectorModule creates a new audit collector.
func NewCollectorModule(name string, config map[string]any) *CollectorModule {
	cfg := parseCollectorConfig(config)
	return &CollectorModule{
		name:    name,
		config:  cfg,
		eventCh: make(chan AuditEvent, cfg.BufferSize*2),
		stopCh:  make(chan struct{}),
	}
}

// RegisterSink adds a sink to the collector.
func (c *CollectorModule) RegisterSink(sink AuditSink) {
	c.sinksMu.Lock()
	defer c.sinksMu.Unlock()
	c.sinks = append(c.sinks, sink)
}

// SetMessagePublisher implements MessageAwareModule.
func (c *CollectorModule) SetMessagePublisher(pub sdk.MessagePublisher) {
	c.pub = pub
}

// SetMessageSubscriber implements MessageAwareModule.
func (c *CollectorModule) SetMessageSubscriber(sub sdk.MessageSubscriber) {
	c.sub = sub
}

func (c *CollectorModule) Init() error { return nil }

func (c *CollectorModule) Start(ctx context.Context) error {
	if c.sub != nil {
		for _, topic := range c.config.Topics {
			t := topic
			if err := c.sub.Subscribe(t, func(payload []byte, metadata map[string]string) error {
				ev := NewAuditEvent(t, payload, metadata)
				select {
				case c.eventCh <- ev:
				default:
					c.droppedCount.Add(1)
					log.Printf("[audit] buffer full, dropping event %s (total dropped: %d)", ev.ID, c.droppedCount.Load())
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}

	c.wg.Add(1)
	go c.flushLoop(ctx)
	return nil
}

func (c *CollectorModule) Stop(ctx context.Context) error {
	close(c.stopCh)

	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("audit collector stop timed out: %w", ctx.Err())
	}

	if c.sub != nil {
		for _, topic := range c.config.Topics {
			_ = c.sub.Unsubscribe(topic)
		}
	}

	c.flush(ctx)

	c.sinksMu.RLock()
	defer c.sinksMu.RUnlock()
	for _, sink := range c.sinks {
		_ = sink.Close()
	}
	return nil
}

func (c *CollectorModule) flushLoop(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Duration(c.config.FlushInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flush(ctx)
		}
	}
}

func (c *CollectorModule) flush(ctx context.Context) {
	var batch []AuditEvent
	for {
		select {
		case ev := <-c.eventCh:
			batch = append(batch, ev)
			if len(batch) >= c.config.BufferSize {
				c.writeBatch(ctx, batch)
				batch = nil
			}
		default:
			if len(batch) > 0 {
				c.writeBatch(ctx, batch)
			}
			return
		}
	}
}

func (c *CollectorModule) writeBatch(ctx context.Context, batch []AuditEvent) {
	c.sinksMu.RLock()
	defer c.sinksMu.RUnlock()
	for _, sink := range c.sinks {
		if err := sink.Write(ctx, batch); err != nil {
			log.Printf("[audit] sink write error: %v", err)
		}
	}
}

// InvokeMethod implements ServiceInvoker for querying events from steps.
func (c *CollectorModule) InvokeMethod(method string, args map[string]any) (map[string]any, error) {
	switch method {
	case "query":
		q := parseQueryArgs(args)
		c.sinksMu.RLock()
		defer c.sinksMu.RUnlock()
		for _, sink := range c.sinks {
			events, err := sink.Query(context.Background(), q)
			if err == nil && len(events) > 0 {
				data, _ := json.Marshal(events)
				return map[string]any{"events": string(data), "count": len(events)}, nil
			}
		}
		return map[string]any{"events": "[]", "count": 0}, nil
	case "stats":
		return map[string]any{
			"dropped":  c.droppedCount.Load(),
			"buffered": len(c.eventCh),
		}, nil
	}
	return nil, nil
}

func parseQueryArgs(args map[string]any) AuditQuery {
	q := AuditQuery{Limit: 100}
	if v, ok := args["since"].(string); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			q.Since = t
		}
	}
	if v, ok := args["until"].(string); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			q.Until = t
		}
	}
	if v, ok := args["eventType"].(string); ok {
		q.EventType = v
	}
	if v, ok := args["workflowId"].(string); ok {
		q.WorkflowID = v
	}
	if v, ok := args["correlationId"].(string); ok {
		q.CorrelationID = v
	}
	if v, ok := args["limit"].(float64); ok {
		q.Limit = int(v)
	}
	return q
}
