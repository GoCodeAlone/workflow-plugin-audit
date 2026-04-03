package internal

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// AuditExportStep exports queried audit events to S3 as a JSONL report.
type AuditExportStep struct {
	name   string
	config map[string]any
}

func NewAuditExportStep(name string, config map[string]any) *AuditExportStep {
	return &AuditExportStep{name: name, config: config}
}

func (s *AuditExportStep) Execute(
	ctx context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	metadata map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	merged := mergeConfigs(s.config, config, current)

	// Get events to export (from previous step output or query)
	var events []AuditEvent
	if evStr, ok := merged["events"].(string); ok {
		if err := json.Unmarshal([]byte(evStr), &events); err != nil {
			return nil, fmt.Errorf("parse events: %w", err)
		}
	}

	if len(events) == 0 {
		return &sdk.StepResult{
			Output: map[string]any{
				"exported": 0,
				"key":      "",
			},
		}, nil
	}

	// Serialize to gzipped JSONL
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			gz.Close()
			return nil, fmt.Errorf("encode event: %w", err)
		}
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("close gzip: %w", err)
	}

	bucket := stringFromMerged(merged, "bucket")
	prefix := stringFromMerged(merged, "prefix")
	if prefix == "" {
		prefix = "audit-exports/"
	}

	key := fmt.Sprintf("%s%s/report-%s.jsonl.gz",
		prefix,
		time.Now().UTC().Format("2006/01/02"),
		uuid.New().String(),
	)

	// If no S3 client, just return the serialized data info
	if bucket == "" {
		return &sdk.StepResult{
			Output: map[string]any{
				"exported":   len(events),
				"key":        key,
				"bytesTotal": buf.Len(),
			},
		}, nil
	}

	// Initialize S3 client from AWS default config if not set (e.g. production)
	client := exportClient
	if client == nil {
		region := stringFromMerged(merged, "region")
		var opts []func(*awsconfig.LoadOptions) error
		if region != "" {
			opts = append(opts, awsconfig.WithRegion(region))
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return &sdk.StepResult{
				Output: map[string]any{"error": "failed to load AWS config: " + err.Error()},
			}, nil
		}
		client = s3.NewFromConfig(cfg)
	}

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(buf.Bytes()),
		ContentType:     aws.String("application/gzip"),
		ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		return nil, fmt.Errorf("upload to S3: %w", err)
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"exported": len(events),
			"bucket":   bucket,
			"key":      key,
		},
	}, nil
}

// exportClient can be set for testing.
var exportClient S3Client

// SetExportClient sets the S3 client for the export step (testing).
func SetExportClient(c S3Client) { exportClient = c }

func stringFromMerged(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
