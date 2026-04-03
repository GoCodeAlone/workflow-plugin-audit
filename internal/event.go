package internal

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// AuditEvent represents a single compliance audit log entry.
type AuditEvent struct {
	ID            string            `json:"id"`
	Timestamp     time.Time         `json:"timestamp"`
	EventType     string            `json:"eventType"`
	WorkflowID    string            `json:"workflowId,omitempty"`
	PipelineID    string            `json:"pipelineId,omitempty"`
	StepName      string            `json:"stepName,omitempty"`
	Actor         string            `json:"actor,omitempty"`
	CorrelationID string            `json:"correlationId,omitempty"`
	Data          map[string]any    `json:"data,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// NewAuditEvent creates an event from an EventBus message.
func NewAuditEvent(eventType string, payload []byte, metadata map[string]string) AuditEvent {
	ev := AuditEvent{
		ID:        uuid.New().String(),
		Timestamp: time.Now().UTC(),
		EventType: eventType,
	}
	if v, ok := metadata["workflowId"]; ok {
		ev.WorkflowID = v
	}
	if v, ok := metadata["pipelineId"]; ok {
		ev.PipelineID = v
	}
	if v, ok := metadata["stepName"]; ok {
		ev.StepName = v
	}
	if v, ok := metadata["actor"]; ok {
		ev.Actor = v
	}
	if v, ok := metadata["correlationId"]; ok {
		ev.CorrelationID = v
	}

	// Attempt to parse payload as structured data
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err == nil {
		ev.Data = data
	} else {
		ev.Data = map[string]any{"raw": string(payload)}
	}

	// Copy remaining metadata as annotations
	annotations := make(map[string]string)
	reserved := map[string]bool{
		"workflowId": true, "pipelineId": true, "stepName": true,
		"actor": true, "correlationId": true,
	}
	for k, v := range metadata {
		if !reserved[k] {
			annotations[k] = v
		}
	}
	if len(annotations) > 0 {
		ev.Annotations = annotations
	}

	return ev
}
