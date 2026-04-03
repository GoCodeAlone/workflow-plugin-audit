package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal"
)

func TestDBSink_WriteAndQuery(t *testing.T) {
	mock := &mockDBInvoker{rows: make(map[string][]map[string]any)}
	sink := internal.NewDBSink(mock, "audit_events")

	now := time.Now().UTC()
	events := []internal.AuditEvent{
		{
			ID:         "evt-1",
			Timestamp:  now,
			EventType:  "workflow.started",
			WorkflowID: "wf-1",
			Actor:      "admin",
			Data:       map[string]any{"key": "value"},
		},
	}

	ctx := context.Background()
	if err := sink.Write(ctx, events); err != nil {
		t.Fatal(err)
	}

	if mock.execCount != 1 {
		t.Errorf("expected 1 exec call, got %d", mock.execCount)
	}
}

type mockDBInvoker struct {
	execCount  int
	queryCount int
	rows       map[string][]map[string]any
}

func (m *mockDBInvoker) InvokeMethod(method string, args map[string]any) (map[string]any, error) {
	switch method {
	case "exec":
		m.execCount++
		return map[string]any{"rowsAffected": 1}, nil
	case "query":
		m.queryCount++
		return map[string]any{"rows": []any{}}, nil
	default:
		return nil, nil
	}
}
