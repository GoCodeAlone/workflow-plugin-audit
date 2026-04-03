package internal

import (
	"context"
	"time"
)

// AuditSink defines a destination for audit events.
type AuditSink interface {
	Write(ctx context.Context, events []AuditEvent) error
	Query(ctx context.Context, q AuditQuery) ([]AuditEvent, error)
	Close() error
}

// AuditQuery specifies filters for querying audit events.
type AuditQuery struct {
	Since         time.Time `json:"since,omitempty"`
	Until         time.Time `json:"until,omitempty"`
	EventType     string    `json:"eventType,omitempty"`
	WorkflowID    string    `json:"workflowId,omitempty"`
	CorrelationID string    `json:"correlationId,omitempty"`
	Limit         int       `json:"limit,omitempty"`
}
