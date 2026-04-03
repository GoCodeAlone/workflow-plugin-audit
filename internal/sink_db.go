package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// DBInvoker abstracts the host engine's database service invocation.
type DBInvoker interface {
	InvokeMethod(method string, args map[string]any) (map[string]any, error)
}

// DBSink writes audit events to the host database via ServiceInvoker.
type DBSink struct {
	invoker   DBInvoker
	tableName string
}

// NewDBSink creates a DB sink using a ServiceInvoker proxy.
func NewDBSink(invoker DBInvoker, tableName string) *DBSink {
	if tableName == "" {
		tableName = "audit_events"
	}
	return &DBSink{invoker: invoker, tableName: tableName}
}

func (d *DBSink) Write(ctx context.Context, events []AuditEvent) error {
	for _, ev := range events {
		dataJSON, _ := json.Marshal(ev.Data)
		annotJSON, _ := json.Marshal(ev.Annotations)

		_, err := d.invoker.InvokeMethod("exec", map[string]any{
			"query": fmt.Sprintf(
				"INSERT INTO %s (id, timestamp, event_type, workflow_id, pipeline_id, step_name, actor, correlation_id, data, annotations) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				d.tableName,
			),
			"args": []any{
				ev.ID, ev.Timestamp.Format(time.RFC3339Nano), ev.EventType,
				ev.WorkflowID, ev.PipelineID, ev.StepName, ev.Actor,
				ev.CorrelationID, string(dataJSON), string(annotJSON),
			},
		})
		if err != nil {
			return fmt.Errorf("insert audit event %s: %w", ev.ID, err)
		}
	}
	return nil
}

func (d *DBSink) Query(ctx context.Context, q AuditQuery) ([]AuditEvent, error) {
	where := "1=1"
	var args []any

	if !q.Since.IsZero() {
		where += " AND timestamp >= ?"
		args = append(args, q.Since.Format(time.RFC3339Nano))
	}
	if !q.Until.IsZero() {
		where += " AND timestamp <= ?"
		args = append(args, q.Until.Format(time.RFC3339Nano))
	}
	if q.EventType != "" {
		where += " AND event_type = ?"
		args = append(args, q.EventType)
	}
	if q.WorkflowID != "" {
		where += " AND workflow_id = ?"
		args = append(args, q.WorkflowID)
	}
	if q.CorrelationID != "" {
		where += " AND correlation_id = ?"
		args = append(args, q.CorrelationID)
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf("SELECT id, timestamp, event_type, workflow_id, pipeline_id, step_name, actor, correlation_id, data, annotations FROM %s WHERE %s ORDER BY timestamp DESC LIMIT %d",
		d.tableName, where, limit)

	result, err := d.invoker.InvokeMethod("query", map[string]any{
		"query": query,
		"args":  args,
	})
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}

	return parseDBRows(result)
}

func parseDBRows(result map[string]any) ([]AuditEvent, error) {
	rowsRaw, ok := result["rows"]
	if !ok {
		return nil, nil
	}
	rows, ok := rowsRaw.([]any)
	if !ok {
		return nil, nil
	}

	var events []AuditEvent
	for _, rowRaw := range rows {
		row, ok := rowRaw.(map[string]any)
		if !ok {
			continue
		}
		ev := AuditEvent{
			ID:            stringVal(row, "id"),
			EventType:     stringVal(row, "event_type"),
			WorkflowID:    stringVal(row, "workflow_id"),
			PipelineID:    stringVal(row, "pipeline_id"),
			StepName:      stringVal(row, "step_name"),
			Actor:         stringVal(row, "actor"),
			CorrelationID: stringVal(row, "correlation_id"),
		}
		if ts := stringVal(row, "timestamp"); ts != "" {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				ev.Timestamp = t
			}
		}
		if d := stringVal(row, "data"); d != "" {
			var data map[string]any
			if json.Unmarshal([]byte(d), &data) == nil {
				ev.Data = data
			}
		}
		if a := stringVal(row, "annotations"); a != "" {
			var ann map[string]string
			if json.Unmarshal([]byte(a), &ann) == nil {
				ev.Annotations = ann
			}
		}
		events = append(events, ev)
	}
	return events, nil
}

func stringVal(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (d *DBSink) Close() error { return nil }
