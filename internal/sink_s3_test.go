package internal_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal"
)

// mockS3Client implements internal.S3Client for testing.
type mockS3Client struct {
	objects map[string][]byte
}

func newMockS3() *mockS3Client {
	return &mockS3Client{objects: make(map[string][]byte)}
}

func (m *mockS3Client) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	data, _ := io.ReadAll(in.Body)
	m.objects[*in.Key] = data
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	var contents []s3types.Object
	prefix := ""
	if in.Prefix != nil {
		prefix = *in.Prefix
	}
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			k := key
			contents = append(contents, s3types.Object{Key: &k})
		}
	}
	return &s3.ListObjectsV2Output{Contents: contents}, nil
}

func (m *mockS3Client) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	data := m.objects[*in.Key]
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func TestS3Sink_WriteAndQuery(t *testing.T) {
	mock := newMockS3()
	sink := internal.NewS3SinkWithClient(mock, "test-bucket", "audit/")

	events := []internal.AuditEvent{
		{
			ID:         "evt-1",
			Timestamp:  time.Now().UTC(),
			EventType:  "workflow.started",
			WorkflowID: "wf-1",
			Actor:      "admin",
		},
		{
			ID:         "evt-2",
			Timestamp:  time.Now().UTC(),
			EventType:  "step.completed",
			WorkflowID: "wf-1",
			StepName:   "validate",
		},
	}

	ctx := context.Background()
	if err := sink.Write(ctx, events); err != nil {
		t.Fatal(err)
	}

	if len(mock.objects) != 1 {
		t.Fatalf("expected 1 S3 object, got %d", len(mock.objects))
	}

	// Verify gzipped JSONL content
	for _, data := range mock.objects {
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		dec := json.NewDecoder(gz)
		var count int
		for dec.More() {
			var ev internal.AuditEvent
			if err := dec.Decode(&ev); err != nil {
				t.Fatal(err)
			}
			count++
		}
		gz.Close()
		if count != 2 {
			t.Errorf("expected 2 events in JSONL, got %d", count)
		}
	}
}

func TestS3Sink_WriteEmpty(t *testing.T) {
	mock := newMockS3()
	sink := internal.NewS3SinkWithClient(mock, "test-bucket", "audit/")

	if err := sink.Write(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(mock.objects) != 0 {
		t.Errorf("expected 0 objects for empty write, got %d", len(mock.objects))
	}
}

func TestS3Sink_Query(t *testing.T) {
	mock := newMockS3()
	sink := internal.NewS3SinkWithClient(mock, "test-bucket", "audit/")

	now := time.Now().UTC()
	events := []internal.AuditEvent{
		{ID: "e1", Timestamp: now, EventType: "workflow.started", WorkflowID: "wf-1"},
		{ID: "e2", Timestamp: now, EventType: "step.completed", WorkflowID: "wf-2"},
	}

	ctx := context.Background()
	_ = sink.Write(ctx, events)

	result, err := sink.Query(ctx, internal.AuditQuery{
		Since:      now.Add(-time.Hour),
		Until:      now.Add(time.Hour),
		WorkflowID: "wf-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 filtered event, got %d", len(result))
	}
	if len(result) > 0 && result[0].ID != "e1" {
		t.Errorf("expected event e1, got %s", result[0].ID)
	}
}
