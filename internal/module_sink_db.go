package internal

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// DBSinkModule wraps a DBSink as a workflow module.
type DBSinkModule struct {
	name      string
	config    map[string]any
	sink      *DBSink
	invoker   DBInvoker
	tableName string
}

// NewDBSinkModule creates a new DB sink module.
func NewDBSinkModule(name string, config map[string]any) *DBSinkModule {
	tableName := stringFromConfig(config, "tableName")
	if tableName == "" {
		tableName = "audit_events"
	}
	return &DBSinkModule{
		name:      name,
		config:    config,
		tableName: tableName,
	}
}

func (m *DBSinkModule) Init() error { return nil }

func (m *DBSinkModule) Start(_ context.Context) error { return nil }

func (m *DBSinkModule) Stop(_ context.Context) error {
	if m.sink != nil {
		return m.sink.Close()
	}
	return nil
}

// SetInvoker sets the host DB invoker for the sink.
func (m *DBSinkModule) SetInvoker(invoker DBInvoker) {
	m.invoker = invoker
	m.sink = NewDBSink(invoker, m.tableName)
}

// Sink returns the underlying AuditSink for registration with the collector.
func (m *DBSinkModule) Sink() AuditSink {
	return m.sink
}

// InvokeMethod implements ServiceInvoker.
func (m *DBSinkModule) InvokeMethod(method string, args map[string]any) (map[string]any, error) {
	switch method {
	case "info":
		return map[string]any{
			"type":      "db",
			"tableName": m.tableName,
		}, nil
	case "set_invoker":
		// Allow wiring the DB invoker from host config
		return map[string]any{"status": "ok"}, nil
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}

var _ sdk.ModuleInstance = (*DBSinkModule)(nil)
var _ sdk.ServiceInvoker = (*DBSinkModule)(nil)
