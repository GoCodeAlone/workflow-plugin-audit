package internal

import (
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

var Manifest = sdk.PluginManifest{
	Name:        "workflow-plugin-audit",
	Version:     "0.1.0",
	Description: "Compliance audit logging (EventBus → S3/DB sinks)",
	Author:      "GoCodeAlone",
}

var moduleTypes = []string{"audit.collector", "audit.sink.s3", "audit.sink.db"}
var stepTypes = []string{"step.audit_query", "step.audit_export", "step.audit_annotate"}

type plugin struct{}

func NewPlugin() sdk.PluginProvider { return &plugin{} }

func (p *plugin) Manifest() sdk.PluginManifest { return Manifest }

func (p *plugin) ModuleTypes() []string { return moduleTypes }

func (p *plugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "audit.collector":
		return NewCollectorModule(name, config), nil
	case "audit.sink.s3":
		return NewS3SinkModule(name, config), nil
	case "audit.sink.db":
		return NewDBSinkModule(name, config), nil
	default:
		return nil, nil
	}
}

func (p *plugin) StepTypes() []string { return stepTypes }

func (p *plugin) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.audit_query":
		return NewAuditQueryStep(name, config), nil
	case "step.audit_export":
		return NewAuditExportStep(name, config), nil
	case "step.audit_annotate":
		return NewAuditAnnotateStep(name, config), nil
	default:
		return nil, nil
	}
}
