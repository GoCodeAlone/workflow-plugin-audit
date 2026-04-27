package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal/contracts"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func typedAuditQuery() sdk.TypedStepHandler[*contracts.AuditQueryConfig, *contracts.AuditQueryInput, *contracts.AuditQueryOutput] {
	return func(ctx context.Context, req sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]) (*sdk.TypedStepResult[*contracts.AuditQueryOutput], error) {
		config, err := queryConfigToMap(req.Config)
		if err != nil {
			return nil, err
		}
		input, err := queryInputToMap(req.Input)
		if err != nil {
			return nil, err
		}
		merged := mergeConfigs(config, input, req.Current)
		if err := validateQueryMap(merged); err != nil {
			return nil, err
		}
		step := NewAuditQueryStep("", config)
		result, err := step.Execute(ctx, req.TriggerData, req.StepOutputs, req.Current, req.Metadata, input)
		if err != nil {
			return nil, err
		}
		output, err := auditQueryOutputFromMap(result.Output)
		if err != nil {
			return nil, err
		}
		return &sdk.TypedStepResult[*contracts.AuditQueryOutput]{Output: output, StopPipeline: result.StopPipeline}, nil
	}
}

func typedAuditExport() sdk.TypedStepHandler[*contracts.AuditExportConfig, *contracts.AuditExportInput, *contracts.AuditExportOutput] {
	return func(ctx context.Context, req sdk.TypedStepRequest[*contracts.AuditExportConfig, *contracts.AuditExportInput]) (*sdk.TypedStepResult[*contracts.AuditExportOutput], error) {
		config := exportConfigToMap(req.Config)
		input, err := exportInputToMap(req.Input)
		if err != nil {
			return nil, err
		}
		step := NewAuditExportStep("", config)
		result, err := step.Execute(ctx, req.TriggerData, req.StepOutputs, req.Current, req.Metadata, input)
		if err != nil {
			return nil, err
		}
		return &sdk.TypedStepResult[*contracts.AuditExportOutput]{Output: auditExportOutputFromMap(result.Output), StopPipeline: result.StopPipeline}, nil
	}
}

func typedAuditAnnotate() sdk.TypedStepHandler[*contracts.AuditAnnotateConfig, *contracts.AuditAnnotateInput, *contracts.AuditAnnotateOutput] {
	return func(ctx context.Context, req sdk.TypedStepRequest[*contracts.AuditAnnotateConfig, *contracts.AuditAnnotateInput]) (*sdk.TypedStepResult[*contracts.AuditAnnotateOutput], error) {
		step := NewAuditAnnotateStep("", annotateConfigToMap(req.Config))
		result, err := step.Execute(ctx, req.TriggerData, req.StepOutputs, req.Current, req.Metadata, annotateInputToMap(req.Input))
		if err != nil {
			return nil, err
		}
		return &sdk.TypedStepResult[*contracts.AuditAnnotateOutput]{Output: auditAnnotateOutputFromMap(result.Output), StopPipeline: result.StopPipeline}, nil
	}
}

func (c *CollectorModule) InvokeTypedMethod(method string, input *anypb.Any) (*anypb.Any, error) {
	switch method {
	case "query":
		req, err := unpackTypedArgs(input, &contracts.QueryRequest{})
		if err != nil {
			return nil, err
		}
		args, err := queryRequestToMap(req)
		if err != nil {
			return nil, err
		}
		out, err := c.InvokeMethod(method, args)
		if err != nil {
			return nil, err
		}
		response, err := queryResponseFromMap(out)
		if err != nil {
			return nil, err
		}
		return anypb.New(response)
	case "stats":
		if _, err := unpackTypedArgs(input, &contracts.StatsRequest{}); err != nil {
			return nil, err
		}
		out, err := c.InvokeMethod(method, nil)
		if err != nil {
			return nil, err
		}
		return anypb.New(statsResponseFromMap(out))
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}

func (m *S3SinkModule) InvokeTypedMethod(method string, input *anypb.Any) (*anypb.Any, error) {
	if method != "info" {
		return nil, fmt.Errorf("unknown method: %s", method)
	}
	if _, err := unpackTypedArgs(input, &contracts.InfoRequest{}); err != nil {
		return nil, err
	}
	out, err := m.InvokeMethod(method, nil)
	if err != nil {
		return nil, err
	}
	return anypb.New(sinkInfoResponseFromMap(out))
}

func (m *DBSinkModule) InvokeTypedMethod(method string, input *anypb.Any) (*anypb.Any, error) {
	if method != "info" {
		return nil, fmt.Errorf("unknown method: %s", method)
	}
	if _, err := unpackTypedArgs(input, &contracts.InfoRequest{}); err != nil {
		return nil, err
	}
	out, err := m.InvokeMethod(method, nil)
	if err != nil {
		return nil, err
	}
	return anypb.New(sinkInfoResponseFromMap(out))
}

func unpackTypedArgs[T proto.Message](input *anypb.Any, target T) (T, error) {
	if input == nil {
		var zero T
		return zero, fmt.Errorf("typed input is required")
	}
	if input.MessageName() != target.ProtoReflect().Descriptor().FullName() {
		var zero T
		return zero, fmt.Errorf("typed input type mismatch: expected %s, got %s", target.ProtoReflect().Descriptor().FullName(), input.MessageName())
	}
	if err := input.UnmarshalTo(target); err != nil {
		var zero T
		return zero, err
	}
	return target, nil
}

func collectorConfigToMap(cfg *contracts.CollectorConfig) (map[string]any, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.GetFlushInterval() < 0 {
		return nil, fmt.Errorf("flush_interval must be positive when set")
	}
	if cfg.GetBufferSize() < 0 {
		return nil, fmt.Errorf("buffer_size must be positive when set")
	}
	out := map[string]any{}
	if len(cfg.GetTopics()) > 0 {
		out["topics"] = stringsToAny(cfg.GetTopics())
	}
	if cfg.GetFlushInterval() != 0 {
		out["flushInterval"] = float64(cfg.GetFlushInterval())
	}
	if cfg.GetBufferSize() != 0 {
		out["bufferSize"] = float64(cfg.GetBufferSize())
	}
	return out, nil
}

func s3ConfigToMap(cfg *contracts.S3SinkConfig) map[string]any {
	if cfg == nil {
		return nil
	}
	return compactMap(map[string]any{"bucket": cfg.GetBucket(), "prefix": cfg.GetPrefix(), "region": cfg.GetRegion()})
}

func dbConfigToMap(cfg *contracts.DBSinkConfig) map[string]any {
	if cfg == nil {
		return nil
	}
	return compactMap(map[string]any{"tableName": cfg.GetTableName()})
}

func queryConfigToMap(cfg *contracts.AuditQueryConfig) (map[string]any, error) {
	if cfg == nil {
		return nil, nil
	}
	return queryFieldsToMap(cfg.GetSince(), cfg.GetUntil(), cfg.GetEventType(), cfg.GetWorkflowId(), cfg.GetCorrelationId(), cfg.Limit), nil
}

func queryInputToMap(input *contracts.AuditQueryInput) (map[string]any, error) {
	if input == nil {
		return nil, nil
	}
	return queryFieldsToMap(input.GetSince(), input.GetUntil(), input.GetEventType(), input.GetWorkflowId(), input.GetCorrelationId(), input.Limit), nil
}

func queryRequestToMap(req *contracts.QueryRequest) (map[string]any, error) {
	if req == nil {
		return nil, nil
	}
	if err := validateQueryFields(req.GetSince(), req.GetUntil(), req.Limit); err != nil {
		return nil, err
	}
	return queryFieldsToMap(req.GetSince(), req.GetUntil(), req.GetEventType(), req.GetWorkflowId(), req.GetCorrelationId(), req.Limit), nil
}

func queryFieldsToMap(since, until, eventType, workflowID, correlationID string, limit *int32) map[string]any {
	values := compactMap(map[string]any{
		"since":         since,
		"until":         until,
		"eventType":     eventType,
		"workflowId":    workflowID,
		"correlationId": correlationID,
	})
	if limit != nil {
		values["limit"] = float64(*limit)
	}
	return values
}

func validateQueryMap(values map[string]any) error {
	if values == nil {
		return nil
	}
	return validateQueryFields(strVal(values, "since"), strVal(values, "until"), queryLimitFromMap(values))
}

func queryLimitFromMap(values map[string]any) *int32 {
	if _, ok := values["limit"]; !ok {
		return nil
	}
	limit := int32(toInt(values["limit"]))
	return &limit
}

func validateQueryFields(since, until string, limit *int32) error {
	if limit != nil && *limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	parsedSince, err := parseOptionalQueryTimestamp("since", since)
	if err != nil {
		return err
	}
	parsedUntil, err := parseOptionalQueryTimestamp("until", until)
	if err != nil {
		return err
	}
	if parsedSince != nil && parsedUntil != nil && parsedSince.After(*parsedUntil) {
		return fmt.Errorf("since timestamp must be before or equal to until timestamp")
	}
	return nil
}

func parseOptionalQueryTimestamp(name, raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, fmt.Errorf("%s timestamp %q: %w", name, raw, err)
	}
	return &parsed, nil
}

func exportConfigToMap(cfg *contracts.AuditExportConfig) map[string]any {
	if cfg == nil {
		return nil
	}
	return compactMap(map[string]any{"bucket": cfg.GetBucket(), "prefix": cfg.GetPrefix(), "region": cfg.GetRegion()})
}

func exportInputToMap(input *contracts.AuditExportInput) (map[string]any, error) {
	if input == nil {
		return nil, nil
	}
	events := make([]AuditEvent, 0, len(input.GetEvents()))
	for _, event := range input.GetEvents() {
		auditEvent, err := protoToAuditEvent(event)
		if err != nil {
			return nil, err
		}
		events = append(events, auditEvent)
	}
	raw, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}
	return compactMap(map[string]any{
		"events": string(raw),
		"bucket": input.GetBucket(),
		"prefix": input.GetPrefix(),
		"region": input.GetRegion(),
	}), nil
}

func annotateConfigToMap(cfg *contracts.AuditAnnotateConfig) map[string]any {
	if cfg == nil {
		return nil
	}
	return compactMap(map[string]any{"annotations": cfg.GetAnnotations(), "key": cfg.GetKey(), "value": cfg.GetValue()})
}

func annotateInputToMap(input *contracts.AuditAnnotateInput) map[string]any {
	if input == nil {
		return nil
	}
	return compactMap(map[string]any{"annotations": input.GetAnnotations(), "key": input.GetKey(), "value": input.GetValue()})
}

func auditQueryOutputFromMap(out map[string]any) (*contracts.AuditQueryOutput, error) {
	response, err := queryResponseFromMap(out)
	if err != nil {
		return nil, err
	}
	return &contracts.AuditQueryOutput{Events: response.Events, Count: response.Count, Error: response.Error}, nil
}

func queryResponseFromMap(out map[string]any) (*contracts.QueryResponse, error) {
	response := &contracts.QueryResponse{Count: int32(toInt(out["count"])), Error: strVal(out, "error")}
	if raw, ok := out["events"].(string); ok && raw != "" {
		var events []AuditEvent
		if err := json.Unmarshal([]byte(raw), &events); err != nil {
			return nil, fmt.Errorf("query response events: %w", err)
		}
		for i := range events {
			response.Events = append(response.Events, auditEventToProto(events[i]))
		}
	}
	return response, nil
}

func auditExportOutputFromMap(out map[string]any) *contracts.AuditExportOutput {
	return &contracts.AuditExportOutput{
		Exported:   int32(toInt(out["exported"])),
		Bucket:     strVal(out, "bucket"),
		Key:        strVal(out, "key"),
		BytesTotal: int32(toInt(out["bytesTotal"])),
		Error:      strVal(out, "error"),
	}
}

func auditAnnotateOutputFromMap(out map[string]any) *contracts.AuditAnnotateOutput {
	return &contracts.AuditAnnotateOutput{
		Annotations: mapStringString(out["annotations"]),
		Count:       int32(toInt(out["count"])),
		Error:       strVal(out, "error"),
	}
}

func statsResponseFromMap(out map[string]any) *contracts.StatsResponse {
	return &contracts.StatsResponse{
		Dropped:  int64(toInt(out["dropped"])),
		Buffered: int32(toInt(out["buffered"])),
		Error:    strVal(out, "error"),
	}
}

func sinkInfoResponseFromMap(out map[string]any) *contracts.SinkInfoResponse {
	return &contracts.SinkInfoResponse{
		Type:      strVal(out, "type"),
		Bucket:    strVal(out, "bucket"),
		Prefix:    strVal(out, "prefix"),
		TableName: strVal(out, "tableName"),
		Error:     strVal(out, "error"),
	}
}

func auditEventToProto(event AuditEvent) *contracts.AuditEvent {
	return &contracts.AuditEvent{
		Id:            event.ID,
		Timestamp:     event.Timestamp.Format(time.RFC3339Nano),
		EventType:     event.EventType,
		WorkflowId:    event.WorkflowID,
		PipelineId:    event.PipelineID,
		StepName:      event.StepName,
		Actor:         event.Actor,
		CorrelationId: event.CorrelationID,
		Data:          structFromMap(event.Data),
		Annotations:   event.Annotations,
	}
}

func protoToAuditEvent(event *contracts.AuditEvent) (AuditEvent, error) {
	if event == nil {
		return AuditEvent{}, nil
	}
	var timestamp time.Time
	if raw := event.GetTimestamp(); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return AuditEvent{}, fmt.Errorf("audit event %q timestamp %q: %w", event.GetId(), raw, err)
		}
		timestamp = parsed
	}
	return AuditEvent{
		ID:            event.GetId(),
		Timestamp:     timestamp,
		EventType:     event.GetEventType(),
		WorkflowID:    event.GetWorkflowId(),
		PipelineID:    event.GetPipelineId(),
		StepName:      event.GetStepName(),
		Actor:         event.GetActor(),
		CorrelationID: event.GetCorrelationId(),
		Data:          structToMap(event.GetData()),
		Annotations:   event.GetAnnotations(),
	}, nil
}

func structFromMap(values map[string]any) *structpb.Struct {
	if len(values) == 0 {
		return nil
	}
	out, err := structpb.NewStruct(values)
	if err != nil {
		return nil
	}
	return out
}

func structToMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	return value.AsMap()
}

func stringsToAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func mapStringString(value any) map[string]string {
	out := map[string]string{}
	switch v := value.(type) {
	case map[string]string:
		for key, item := range v {
			out[key] = item
		}
	case map[string]any:
		for key, item := range v {
			if str, ok := item.(string); ok {
				out[key] = str
			}
		}
	}
	return out
}

func strVal(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	if value, ok := values[key].(string); ok {
		return value
	}
	return ""
}

func toInt(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	default:
		return 0
	}
}

func compactMap(values map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range values {
		if isZeroTypedValue(value) {
			continue
		}
		out[key] = value
	}
	return out
}

func isZeroTypedValue(value any) bool {
	switch v := value.(type) {
	case string:
		return v == ""
	case float64:
		return v == 0
	case int32:
		return v == 0
	case []AuditEvent:
		return len(v) == 0
	case map[string]string:
		return len(v) == 0
	case nil:
		return true
	default:
		return false
	}
}
