package internal

import (
	"context"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// AuditAnnotateStep adds key-value annotations to the current execution context.
type AuditAnnotateStep struct {
	name   string
	config map[string]any
}

func NewAuditAnnotateStep(name string, config map[string]any) *AuditAnnotateStep {
	return &AuditAnnotateStep{name: name, config: config}
}

func (s *AuditAnnotateStep) Execute(
	_ context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	metadata map[string]any,
	config map[string]any,
) (*sdk.StepResult, error) {
	merged := mergeConfigs(s.config, config, current)

	annotations := make(map[string]string)

	// Copy annotations from config
	if ann, ok := merged["annotations"]; ok {
		switch v := ann.(type) {
		case map[string]any:
			for k, val := range v {
				if str, ok := val.(string); ok {
					annotations[k] = str
				}
			}
		case map[string]string:
			for k, val := range v {
				annotations[k] = val
			}
		}
	}

	// Also support individual key/value in config
	if key, ok := merged["key"].(string); ok {
		if val, ok := merged["value"].(string); ok {
			annotations[key] = val
		}
	}

	// Write annotations into metadata (carried through pipeline)
	if metadata != nil {
		for k, v := range annotations {
			metadata["audit."+k] = v
		}
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"annotations": annotations,
			"count":       len(annotations),
		},
	}, nil
}
