package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// AuditQueryStep queries audit events from configured sinks.
type AuditQueryStep struct {
	name   string
	config map[string]any
}

func NewAuditQueryStep(name string, config map[string]any) *AuditQueryStep {
	return &AuditQueryStep{name: name, config: config}
}

func (s *AuditQueryStep) Execute(
	ctx context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	metadata map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	q := AuditQuery{Limit: 100}

	merged := mergeConfigs(s.config, config, current)

	if v, ok := merged["since"].(string); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			q.Since = t
		}
	}
	if v, ok := merged["until"].(string); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			q.Until = t
		}
	}
	if v, ok := merged["eventType"].(string); ok {
		q.EventType = v
	}
	if v, ok := merged["workflowId"].(string); ok {
		q.WorkflowID = v
	}
	if v, ok := merged["correlationId"].(string); ok {
		q.CorrelationID = v
	}
	if v, ok := merged["limit"].(float64); ok {
		q.Limit = int(v)
	}

	sink := resolveMemorySink()
	if sink == nil {
		return &sdk.StepResult{
			Output: map[string]any{
				"events": "[]",
				"count":  0,
				"error":  "no sink available",
			},
		}, nil
	}

	events, err := sink.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("audit query: %w", err)
	}

	data, _ := json.Marshal(events)
	return &sdk.StepResult{
		Output: map[string]any{
			"events": string(data),
			"count":  len(events),
		},
	}, nil
}

func mergeConfigs(configs ...map[string]any) map[string]any {
	result := make(map[string]any)
	for _, c := range configs {
		for k, v := range c {
			result[k] = v
		}
	}
	return result
}

// resolveMemorySink returns the global memory sink (used in tests).
// In production, queries go through the collector module's ServiceInvoker.
var globalSink AuditSink

func SetGlobalSink(s AuditSink) { globalSink = s }

func resolveMemorySink() AuditSink { return globalSink }
