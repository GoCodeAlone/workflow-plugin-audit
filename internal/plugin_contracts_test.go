package internal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal/contracts"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestPluginImplementsStrictContractProviders(t *testing.T) {
	provider := NewPlugin()
	if _, ok := provider.(sdk.TypedModuleProvider); !ok {
		t.Fatal("expected TypedModuleProvider")
	}
	if _, ok := provider.(sdk.TypedStepProvider); !ok {
		t.Fatal("expected TypedStepProvider")
	}
	if _, ok := provider.(sdk.ContractProvider); !ok {
		t.Fatal("expected ContractProvider")
	}
}

func TestContractRegistryDeclaresStrictModuleStepAndServiceContracts(t *testing.T) {
	provider := NewPlugin().(sdk.ContractProvider)
	registry := provider.ContractRegistry()
	if registry == nil {
		t.Fatal("expected contract registry")
	}
	if registry.FileDescriptorSet == nil || len(registry.FileDescriptorSet.File) == 0 {
		t.Fatal("expected file descriptor set")
	}
	files, err := protodesc.NewFiles(registry.FileDescriptorSet)
	if err != nil {
		t.Fatalf("descriptor set: %v", err)
	}

	manifestContracts := loadManifestContracts(t)
	contractsByKey := make(map[string]*pb.ContractDescriptor, len(registry.Contracts))
	for _, contract := range registry.Contracts {
		if contract.Mode != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
			t.Fatalf("%s mode = %s, want strict proto", contractKey(contract), contract.Mode)
		}
		for _, name := range []string{contract.ConfigMessage, contract.InputMessage, contract.OutputMessage} {
			if name == "" {
				continue
			}
			if _, err := files.FindDescriptorByName(protoreflect.FullName(name)); err != nil {
				t.Fatalf("%s references unknown message %s: %v", contractKey(contract), name, err)
			}
		}
		key := contractKey(contract)
		if _, exists := contractsByKey[key]; exists {
			t.Fatalf("duplicate runtime contract %q", key)
		}
		contractsByKey[key] = contract
		want, ok := manifestContracts[key]
		if !ok {
			t.Fatalf("%s missing from plugin.contracts.json", key)
		}
		if want.ConfigMessage != contract.ConfigMessage || want.InputMessage != contract.InputMessage || want.OutputMessage != contract.OutputMessage {
			t.Fatalf("%s manifest contract = %#v, runtime = %#v", key, want, contract)
		}
	}
	if len(contractsByKey) != len(manifestContracts) {
		t.Fatalf("runtime contract count = %d, manifest = %d", len(contractsByKey), len(manifestContracts))
	}

	wantKeys := []string{
		"module:audit.collector",
		"module:audit.sink.s3",
		"module:audit.sink.db",
		"step:step.audit_query",
		"step:step.audit_export",
		"step:step.audit_annotate",
		"service:audit.collector/query",
		"service:audit.collector/stats",
		"service:audit.sink.s3/info",
		"service:audit.sink.db/info",
	}
	for _, key := range wantKeys {
		if _, ok := contractsByKey[key]; !ok {
			t.Fatalf("missing runtime contract %q", key)
		}
	}
}

func TestTypeListsAreDefensiveCopies(t *testing.T) {
	provider := NewPlugin()
	moduleProvider := provider.(sdk.ModuleProvider)
	typedModuleProvider := provider.(sdk.TypedModuleProvider)
	stepProvider := provider.(sdk.StepProvider)
	typedStepProvider := provider.(sdk.TypedStepProvider)

	moduleTypes := moduleProvider.ModuleTypes()
	moduleTypes[0] = "mutated"
	if got := moduleProvider.ModuleTypes()[0]; got == "mutated" {
		t.Fatal("ModuleTypes exposed mutable package-level slice")
	}

	typedModuleTypes := typedModuleProvider.TypedModuleTypes()
	typedModuleTypes[0] = "mutated"
	if got := typedModuleProvider.TypedModuleTypes()[0]; got == "mutated" {
		t.Fatal("TypedModuleTypes exposed mutable package-level slice")
	}

	stepTypes := stepProvider.StepTypes()
	stepTypes[0] = "mutated"
	if got := stepProvider.StepTypes()[0]; got == "mutated" {
		t.Fatal("StepTypes exposed mutable package-level slice")
	}

	typedStepTypes := typedStepProvider.TypedStepTypes()
	typedStepTypes[0] = "mutated"
	if got := typedStepProvider.TypedStepTypes()[0]; got == "mutated" {
		t.Fatal("TypedStepTypes exposed mutable package-level slice")
	}
}

func TestTypedAuditModuleProviderValidatesConfigAndServices(t *testing.T) {
	provider := NewPlugin().(sdk.TypedModuleProvider)
	config, err := anypb.New(&contracts.CollectorConfig{Topics: []string{"workflow.*"}, FlushInterval: 1, BufferSize: 10})
	if err != nil {
		t.Fatalf("pack config: %v", err)
	}
	module, err := provider.CreateTypedModule("audit.collector", "collector", config)
	if err != nil {
		t.Fatalf("CreateTypedModule: %v", err)
	}
	invoker, ok := module.(sdk.TypedServiceInvoker)
	if !ok {
		t.Fatal("expected TypedServiceInvoker")
	}
	statsInput, err := anypb.New(&contracts.StatsRequest{})
	if err != nil {
		t.Fatalf("pack stats input: %v", err)
	}
	statsOutput, err := invoker.InvokeTypedMethod("stats", statsInput)
	if err != nil {
		t.Fatalf("InvokeTypedMethod(stats): %v", err)
	}
	var stats contracts.StatsResponse
	if err := statsOutput.UnmarshalTo(&stats); err != nil {
		t.Fatalf("unpack stats output: %v", err)
	}
	if stats.GetBuffered() != 0 || stats.GetDropped() != 0 {
		t.Fatalf("stats = buffered %d dropped %d, want zeroes", stats.GetBuffered(), stats.GetDropped())
	}

	wrongConfig, err := anypb.New(&contracts.S3SinkConfig{Bucket: "audit"})
	if err != nil {
		t.Fatalf("pack wrong config: %v", err)
	}
	if _, err := provider.CreateTypedModule("audit.collector", "collector", wrongConfig); err == nil {
		t.Fatal("CreateTypedModule accepted wrong typed config")
	}

	negativeBuffer, err := anypb.New(&contracts.CollectorConfig{BufferSize: -1})
	if err != nil {
		t.Fatalf("pack negative buffer config: %v", err)
	}
	if _, err := provider.CreateTypedModule("audit.collector", "collector", negativeBuffer); err == nil {
		t.Fatal("CreateTypedModule accepted negative buffer_size")
	}

	negativeFlush, err := anypb.New(&contracts.CollectorConfig{FlushInterval: -1})
	if err != nil {
		t.Fatalf("pack negative flush config: %v", err)
	}
	if _, err := provider.CreateTypedModule("audit.collector", "collector", negativeFlush); err == nil {
		t.Fatal("CreateTypedModule accepted negative flush_interval")
	}
}

func TestTypedAuditAnnotateStepMergesConfigAndInput(t *testing.T) {
	result, err := typedAuditAnnotate()(context.Background(), sdk.TypedStepRequest[*contracts.AuditAnnotateConfig, *contracts.AuditAnnotateInput]{
		Config: &contracts.AuditAnnotateConfig{
			Annotations: map[string]string{"compliance": "SOC2"},
		},
		Input: &contracts.AuditAnnotateInput{
			Key:   "region",
			Value: "us-east-1",
		},
		Metadata: map[string]any{},
	})
	if err != nil {
		t.Fatalf("typedAuditAnnotate: %v", err)
	}
	if result == nil || result.Output == nil {
		t.Fatal("expected typed output")
	}
	if result.Output.GetCount() != 2 {
		t.Fatalf("count = %d, want 2", result.Output.GetCount())
	}
	if got := result.Output.GetAnnotations()["compliance"]; got != "SOC2" {
		t.Fatalf("compliance = %q, want SOC2", got)
	}
	if got := result.Output.GetAnnotations()["region"]; got != "us-east-1" {
		t.Fatalf("region = %q, want us-east-1", got)
	}
}

func TestTypedAuditExportRejectsInvalidEventTimestamp(t *testing.T) {
	_, err := typedAuditExport()(context.Background(), sdk.TypedStepRequest[*contracts.AuditExportConfig, *contracts.AuditExportInput]{
		Input: &contracts.AuditExportInput{
			Events: []*contracts.AuditEvent{{
				Id:        "event-1",
				Timestamp: "not-a-timestamp",
				EventType: "workflow.started",
			}},
		},
	})
	if err == nil {
		t.Fatal("typedAuditExport accepted invalid event timestamp")
	}
}

func TestTypedAuditQueryRejectsInvalidTimeBounds(t *testing.T) {
	negativeLimit := int32(-1)
	_, err := typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Config: &contracts.AuditQueryConfig{Since: "not-a-timestamp"},
		Input:  &contracts.AuditQueryInput{Since: "2026-04-27T10:00:00Z"},
	})
	if err != nil {
		t.Fatalf("typedAuditQuery rejected invalid config since overridden by input: %v", err)
	}

	_, err = typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Config: &contracts.AuditQueryConfig{Since: "not-a-timestamp"},
	})
	if err == nil {
		t.Fatal("typedAuditQuery accepted invalid config since timestamp")
	}

	_, err = typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Input: &contracts.AuditQueryInput{Until: "not-a-timestamp"},
	})
	if err == nil {
		t.Fatal("typedAuditQuery accepted invalid input until timestamp")
	}

	_, err = typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Config: &contracts.AuditQueryConfig{
			Since: "2026-04-27T12:00:00Z",
			Until: "2026-04-27T11:00:00Z",
		},
	})
	if err == nil {
		t.Fatal("typedAuditQuery accepted since timestamp after until timestamp")
	}

	_, err = typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Config: &contracts.AuditQueryConfig{Since: "2026-04-27T12:00:00Z"},
		Input:  &contracts.AuditQueryInput{Until: "2026-04-27T11:00:00Z"},
	})
	if err == nil {
		t.Fatal("typedAuditQuery accepted merged since timestamp after until timestamp")
	}

	_, err = typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Config:  &contracts.AuditQueryConfig{Since: "2026-04-27T10:00:00Z"},
		Input:   &contracts.AuditQueryInput{Until: "2026-04-27T11:00:00Z"},
		Current: map[string]any{"since": "not-a-timestamp"},
	})
	if err == nil {
		t.Fatal("typedAuditQuery accepted current override with invalid since timestamp")
	}

	_, err = typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Config:  &contracts.AuditQueryConfig{Since: "2026-04-27T10:00:00Z"},
		Input:   &contracts.AuditQueryInput{},
		Current: map[string]any{"until": "2026-04-27T09:00:00Z"},
	})
	if err == nil {
		t.Fatal("typedAuditQuery accepted current override that made since after until")
	}

	_, err = typedAuditQuery()(context.Background(), sdk.TypedStepRequest[*contracts.AuditQueryConfig, *contracts.AuditQueryInput]{
		Input: &contracts.AuditQueryInput{Limit: &negativeLimit},
	})
	if err == nil {
		t.Fatal("typedAuditQuery accepted negative limit")
	}
}

func TestTypedAuditQueryPreservesExplicitZeroLimit(t *testing.T) {
	zero := int32(0)
	ten := int32(10)
	config, err := queryConfigToMap(&contracts.AuditQueryConfig{Limit: &ten})
	if err != nil {
		t.Fatalf("queryConfigToMap: %v", err)
	}
	input, err := queryInputToMap(&contracts.AuditQueryInput{Limit: &zero})
	if err != nil {
		t.Fatalf("queryInputToMap: %v", err)
	}
	merged := mergeConfigs(config, input)
	if got, ok := merged["limit"].(float64); !ok || got != 0 {
		t.Fatalf("merged limit = %#v, want explicit zero", merged["limit"])
	}
}

func TestQueryResponseFromMapRejectsMalformedEvents(t *testing.T) {
	if _, err := queryResponseFromMap(map[string]any{"events": "not-json"}); err == nil {
		t.Fatal("queryResponseFromMap accepted malformed events JSON")
	}
}

type manifestContract struct {
	Mode          string `json:"mode"`
	ConfigMessage string `json:"config"`
	InputMessage  string `json:"input"`
	OutputMessage string `json:"output"`
}

func loadManifestContracts(t *testing.T) map[string]manifestContract {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "plugin.contracts.json"))
	if err != nil {
		t.Fatalf("read plugin.contracts.json: %v", err)
	}
	var manifest struct {
		Version   string `json:"version"`
		Contracts []struct {
			Kind        string `json:"kind"`
			Type        string `json:"type"`
			ServiceName string `json:"serviceName"`
			Method      string `json:"method"`
			manifestContract
		} `json:"contracts"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("parse plugin.contracts.json: %v", err)
	}
	if manifest.Version != "v1" {
		t.Fatalf("plugin.contracts.json version = %q, want v1", manifest.Version)
	}
	contracts := make(map[string]manifestContract, len(manifest.Contracts))
	for _, contract := range manifest.Contracts {
		if contract.Mode != "strict" {
			t.Fatalf("%s mode = %q, want strict", contract.Type, contract.Mode)
		}
		var key string
		switch contract.Kind {
		case "module":
			key = "module:" + contract.Type
		case "step":
			key = "step:" + contract.Type
		case "service_method":
			key = "service:" + contract.ServiceName + "/" + contract.Method
		default:
			t.Fatalf("unexpected contract kind %q in plugin.contracts.json", contract.Kind)
		}
		if _, exists := contracts[key]; exists {
			t.Fatalf("duplicate contract %q in plugin.contracts.json", key)
		}
		contracts[key] = contract.manifestContract
	}
	return contracts
}

func contractKey(contract *pb.ContractDescriptor) string {
	switch contract.Kind {
	case pb.ContractKind_CONTRACT_KIND_MODULE:
		return "module:" + contract.ModuleType
	case pb.ContractKind_CONTRACT_KIND_STEP:
		return "step:" + contract.StepType
	case pb.ContractKind_CONTRACT_KIND_SERVICE:
		return "service:" + contract.ServiceName + "/" + contract.Method
	default:
		return contract.Kind.String()
	}
}
