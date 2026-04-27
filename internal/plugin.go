package internal

import (
	"fmt"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal/contracts"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Version is set at build time via -ldflags
// "-X github.com/GoCodeAlone/workflow-plugin-audit/internal.Version=X.Y.Z".
// Default is a bare semver so plugin loaders that validate semver accept
// unreleased dev builds; goreleaser overrides with the real release tag.
var Version = "0.0.0"

var Manifest = sdk.PluginManifest{
	Name:        "workflow-plugin-audit",
	Version:     Version,
	Description: "Compliance audit logging (EventBus → S3/DB sinks)",
	Author:      "GoCodeAlone",
}

var moduleTypes = []string{"audit.collector", "audit.sink.s3", "audit.sink.db"}
var stepTypes = []string{"step.audit_query", "step.audit_export", "step.audit_annotate"}

type plugin struct{}

func NewPlugin() sdk.PluginProvider { return &plugin{} }

func (p *plugin) Manifest() sdk.PluginManifest { return Manifest }

func (p *plugin) ModuleTypes() []string { return append([]string(nil), moduleTypes...) }

func (p *plugin) TypedModuleTypes() []string { return append([]string(nil), moduleTypes...) }

func (p *plugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "audit.collector":
		return NewCollectorModule(name, config), nil
	case "audit.sink.s3":
		return NewS3SinkModule(name, config), nil
	case "audit.sink.db":
		return NewDBSinkModule(name, config), nil
	default:
		return nil, fmt.Errorf("audit plugin: unknown module type %q", typeName)
	}
}

func (p *plugin) CreateTypedModule(typeName, name string, config *anypb.Any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "audit.collector":
		factory := sdk.NewTypedModuleFactory(typeName, &contracts.CollectorConfig{}, func(name string, cfg *contracts.CollectorConfig) (sdk.ModuleInstance, error) {
			config, err := collectorConfigToMap(cfg)
			if err != nil {
				return nil, err
			}
			return NewCollectorModule(name, config), nil
		})
		return factory.CreateTypedModule(typeName, name, config)
	case "audit.sink.s3":
		factory := sdk.NewTypedModuleFactory(typeName, &contracts.S3SinkConfig{}, func(name string, cfg *contracts.S3SinkConfig) (sdk.ModuleInstance, error) {
			return NewS3SinkModule(name, s3ConfigToMap(cfg)), nil
		})
		return factory.CreateTypedModule(typeName, name, config)
	case "audit.sink.db":
		factory := sdk.NewTypedModuleFactory(typeName, &contracts.DBSinkConfig{}, func(name string, cfg *contracts.DBSinkConfig) (sdk.ModuleInstance, error) {
			return NewDBSinkModule(name, dbConfigToMap(cfg)), nil
		})
		return factory.CreateTypedModule(typeName, name, config)
	default:
		return nil, fmt.Errorf("audit plugin: unknown module type %q", typeName)
	}
}

func (p *plugin) StepTypes() []string { return append([]string(nil), stepTypes...) }

func (p *plugin) TypedStepTypes() []string { return append([]string(nil), stepTypes...) }

func (p *plugin) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.audit_query":
		return NewAuditQueryStep(name, config), nil
	case "step.audit_export":
		return NewAuditExportStep(name, config), nil
	case "step.audit_annotate":
		return NewAuditAnnotateStep(name, config), nil
	default:
		return nil, fmt.Errorf("audit plugin: unknown step type %q", typeName)
	}
}

func (p *plugin) CreateTypedStep(typeName, name string, config *anypb.Any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.audit_query":
		factory := sdk.NewTypedStepFactory(typeName, &contracts.AuditQueryConfig{}, &contracts.AuditQueryInput{}, typedAuditQuery())
		return factory.CreateTypedStep(typeName, name, config)
	case "step.audit_export":
		factory := sdk.NewTypedStepFactory(typeName, &contracts.AuditExportConfig{}, &contracts.AuditExportInput{}, typedAuditExport())
		return factory.CreateTypedStep(typeName, name, config)
	case "step.audit_annotate":
		factory := sdk.NewTypedStepFactory(typeName, &contracts.AuditAnnotateConfig{}, &contracts.AuditAnnotateInput{}, typedAuditAnnotate())
		return factory.CreateTypedStep(typeName, name, config)
	default:
		return nil, fmt.Errorf("audit plugin: unknown step type %q", typeName)
	}
}

func (p *plugin) ContractRegistry() *pb.ContractRegistry {
	return &pb.ContractRegistry{
		FileDescriptorSet: &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(structpb.File_google_protobuf_struct_proto),
			protodesc.ToFileDescriptorProto(contracts.File_internal_contracts_audit_proto),
		}},
		Contracts: []*pb.ContractDescriptor{
			moduleContract("audit.collector", "CollectorConfig"),
			moduleContract("audit.sink.s3", "S3SinkConfig"),
			moduleContract("audit.sink.db", "DBSinkConfig"),
			stepContract("step.audit_query", "AuditQueryConfig", "AuditQueryInput", "AuditQueryOutput"),
			stepContract("step.audit_export", "AuditExportConfig", "AuditExportInput", "AuditExportOutput"),
			stepContract("step.audit_annotate", "AuditAnnotateConfig", "AuditAnnotateInput", "AuditAnnotateOutput"),
			serviceContract("audit.collector", "query", "QueryRequest", "QueryResponse"),
			serviceContract("audit.collector", "stats", "StatsRequest", "StatsResponse"),
			serviceContract("audit.sink.s3", "info", "InfoRequest", "SinkInfoResponse"),
			serviceContract("audit.sink.db", "info", "InfoRequest", "SinkInfoResponse"),
		},
	}
}

func moduleContract(moduleType, configMessage string) *pb.ContractDescriptor {
	const pkg = "workflow.plugins.audit.v1."
	return &pb.ContractDescriptor{
		Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
		ModuleType:    moduleType,
		ConfigMessage: pkg + configMessage,
		Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
	}
}

func stepContract(stepType, configMessage, inputMessage, outputMessage string) *pb.ContractDescriptor {
	const pkg = "workflow.plugins.audit.v1."
	return &pb.ContractDescriptor{
		Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
		StepType:      stepType,
		ConfigMessage: pkg + configMessage,
		InputMessage:  pkg + inputMessage,
		OutputMessage: pkg + outputMessage,
		Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
	}
}

func serviceContract(moduleType, method, inputMessage, outputMessage string) *pb.ContractDescriptor {
	const pkg = "workflow.plugins.audit.v1."
	return &pb.ContractDescriptor{
		Kind:          pb.ContractKind_CONTRACT_KIND_SERVICE,
		ModuleType:    moduleType,
		ServiceName:   moduleType,
		Method:        method,
		InputMessage:  pkg + inputMessage,
		OutputMessage: pkg + outputMessage,
		Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
	}
}
