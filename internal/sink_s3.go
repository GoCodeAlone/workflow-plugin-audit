package internal

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// S3Client abstracts the S3 API for testing.
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3Sink writes audit events as gzipped JSONL to S3.
type S3Sink struct {
	client S3Client
	bucket string
	prefix string
}

// S3SinkConfig configures the S3 sink.
type S3SinkConfig struct {
	Bucket string
	Prefix string
	Region string
}

// NewS3Sink creates an S3 sink with real AWS credentials.
func NewS3Sink(ctx context.Context, cfg S3SinkConfig) (*S3Sink, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return &S3Sink{
		client: s3.NewFromConfig(awsCfg),
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// NewS3SinkWithClient creates an S3 sink with a custom client (for testing).
func NewS3SinkWithClient(client S3Client, bucket, prefix string) *S3Sink {
	return &S3Sink{client: client, bucket: bucket, prefix: prefix}
}

func (s *S3Sink) Write(ctx context.Context, events []AuditEvent) error {
	if len(events) == 0 {
		return nil
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			gz.Close()
			return fmt.Errorf("encode event: %w", err)
		}
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("close gzip: %w", err)
	}

	now := events[0].Timestamp
	key := fmt.Sprintf("%s%s/events-%s.jsonl.gz",
		s.prefix,
		now.Format("2006/01/02"),
		uuid.New().String(),
	)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(buf.Bytes()),
		ContentType:     aws.String("application/gzip"),
		ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

func (s *S3Sink) Query(ctx context.Context, q AuditQuery) ([]AuditEvent, error) {
	// List objects by date prefix, download, decompress, and filter
	var allEvents []AuditEvent
	since := q.Since
	if since.IsZero() {
		since = time.Now().AddDate(0, 0, -7) // default last 7 days
	}
	until := q.Until
	if until.IsZero() {
		until = time.Now()
	}

	for d := since; !d.After(until); d = d.AddDate(0, 0, 1) {
		prefix := fmt.Sprintf("%s%s/", s.prefix, d.Format("2006/01/02"))
		listOut, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
		})
		if err != nil {
			return nil, fmt.Errorf("list objects %s: %w", prefix, err)
		}

		for _, obj := range listOut.Contents {
			events, err := s.downloadAndParse(ctx, *obj.Key)
			if err != nil {
				return nil, err
			}
			for _, ev := range events {
				if matchesQuery(ev, q) {
					allEvents = append(allEvents, ev)
					if q.Limit > 0 && len(allEvents) >= q.Limit {
						return allEvents, nil
					}
				}
			}
		}
	}
	return allEvents, nil
}

func (s *S3Sink) downloadAndParse(ctx context.Context, key string) ([]AuditEvent, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer out.Body.Close()

	var reader io.Reader = out.Body
	if strings.HasSuffix(key, ".gz") {
		gz, err := gzip.NewReader(out.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip reader %s: %w", key, err)
		}
		defer gz.Close()
		reader = gz
	}

	var events []AuditEvent
	dec := json.NewDecoder(reader)
	for dec.More() {
		var ev AuditEvent
		if err := dec.Decode(&ev); err != nil {
			break // partial reads are acceptable
		}
		events = append(events, ev)
	}
	return events, nil
}

func (s *S3Sink) Close() error { return nil }

func matchesQuery(ev AuditEvent, q AuditQuery) bool {
	if !q.Since.IsZero() && ev.Timestamp.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && ev.Timestamp.After(q.Until) {
		return false
	}
	if q.EventType != "" && ev.EventType != q.EventType {
		return false
	}
	if q.WorkflowID != "" && ev.WorkflowID != q.WorkflowID {
		return false
	}
	if q.CorrelationID != "" && ev.CorrelationID != q.CorrelationID {
		return false
	}
	return true
}
