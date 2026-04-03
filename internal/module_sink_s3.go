package internal

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// S3SinkModule wraps an S3Sink as a workflow module.
type S3SinkModule struct {
	name   string
	config map[string]any
	sink   *S3Sink
}

// NewS3SinkModule creates an S3 sink module.
func NewS3SinkModule(name string, config map[string]any) *S3SinkModule {
	return &S3SinkModule{name: name, config: config}
}

func (m *S3SinkModule) Init() error {
	bucket := stringFromConfig(m.config, "bucket")
	if bucket == "" {
		return fmt.Errorf("audit.sink.s3: bucket is required")
	}
	return nil
}

func (m *S3SinkModule) Start(ctx context.Context) error {
	sink, err := NewS3Sink(ctx, S3SinkConfig{
		Bucket: stringFromConfig(m.config, "bucket"),
		Prefix: stringFromConfig(m.config, "prefix"),
		Region: stringFromConfig(m.config, "region"),
	})
	if err != nil {
		return fmt.Errorf("create S3 sink: %w", err)
	}
	m.sink = sink
	return nil
}

func (m *S3SinkModule) Stop(_ context.Context) error {
	if m.sink != nil {
		return m.sink.Close()
	}
	return nil
}

// Sink returns the underlying AuditSink for registration with the collector.
func (m *S3SinkModule) Sink() AuditSink {
	return m.sink
}

// InvokeMethod implements ServiceInvoker.
func (m *S3SinkModule) InvokeMethod(method string, args map[string]any) (map[string]any, error) {
	if method == "info" {
		return map[string]any{
			"type":   "s3",
			"bucket": stringFromConfig(m.config, "bucket"),
			"prefix": stringFromConfig(m.config, "prefix"),
		}, nil
	}
	return nil, fmt.Errorf("unknown method: %s", method)
}

var _ sdk.ModuleInstance = (*S3SinkModule)(nil)
var _ sdk.ServiceInvoker = (*S3SinkModule)(nil)

func stringFromConfig(cfg map[string]any, key string) string {
	if v, ok := cfg[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
