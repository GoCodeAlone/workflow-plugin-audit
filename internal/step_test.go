package internal_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal"
)

func TestAuditQueryStep_Execute(t *testing.T) {
	sink := internal.NewMemorySink()
	now := time.Now().UTC()

	// Pre-populate sink
	_ = sink.Write(context.Background(), []internal.AuditEvent{
		{ID: "e1", Timestamp: now, EventType: "workflow.started", WorkflowID: "wf-1"},
		{ID: "e2", Timestamp: now, EventType: "step.completed", WorkflowID: "wf-2"},
	})

	internal.SetGlobalSink(sink)
	defer internal.SetGlobalSink(nil)

	step := internal.NewAuditQueryStep("test-query", map[string]any{})

	result, err := step.Execute(
		context.Background(),
		nil,
		nil,
		map[string]any{
			"workflowId": "wf-1",
			"since":      now.Add(-time.Hour).Format(time.RFC3339),
			"until":      now.Add(time.Hour).Format(time.RFC3339),
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	count, ok := result.Output["count"].(int)
	if !ok {
		t.Fatal("expected count in output")
	}
	if count != 1 {
		t.Errorf("expected 1 event, got %d", count)
	}
}

func TestAuditExportStep_Execute(t *testing.T) {
	now := time.Now().UTC()
	events := []internal.AuditEvent{
		{ID: "e1", Timestamp: now, EventType: "workflow.started"},
	}
	evJSON, _ := json.Marshal(events)

	step := internal.NewAuditExportStep("test-export", map[string]any{})

	result, err := step.Execute(
		context.Background(),
		nil,
		nil,
		map[string]any{
			"events": string(evJSON),
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	exported, ok := result.Output["exported"].(int)
	if !ok {
		t.Fatal("expected exported count in output")
	}
	if exported != 1 {
		t.Errorf("expected 1 exported event, got %d", exported)
	}

	// Should have key even without bucket
	key, ok := result.Output["key"].(string)
	if !ok || key == "" {
		t.Error("expected non-empty key in output")
	}
}

func TestAuditAnnotateStep_Execute(t *testing.T) {
	step := internal.NewAuditAnnotateStep("test-annotate", map[string]any{})

	metadata := map[string]any{}

	result, err := step.Execute(
		context.Background(),
		nil,
		nil,
		map[string]any{
			"annotations": map[string]any{
				"compliance": "SOC2",
				"region":     "us-east-1",
			},
		},
		metadata,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	count, ok := result.Output["count"].(int)
	if !ok {
		t.Fatal("expected count in output")
	}
	if count != 2 {
		t.Errorf("expected 2 annotations, got %d", count)
	}

	if metadata["audit.compliance"] != "SOC2" {
		t.Errorf("expected audit.compliance=SOC2, got %v", metadata["audit.compliance"])
	}
	if metadata["audit.region"] != "us-east-1" {
		t.Errorf("expected audit.region=us-east-1, got %v", metadata["audit.region"])
	}
}

func TestAuditAnnotateStep_KeyValue(t *testing.T) {
	step := internal.NewAuditAnnotateStep("test-annotate", map[string]any{})

	metadata := map[string]any{}

	result, err := step.Execute(
		context.Background(),
		nil,
		nil,
		map[string]any{
			"key":   "env",
			"value": "production",
		},
		metadata,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	count := result.Output["count"].(int)
	if count != 1 {
		t.Errorf("expected 1 annotation, got %d", count)
	}

	if metadata["audit.env"] != "production" {
		t.Errorf("expected audit.env=production, got %v", metadata["audit.env"])
	}
}
