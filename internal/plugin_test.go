package internal_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-audit/internal"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func TestNewPlugin_ImplementsPluginProvider(t *testing.T) {
	var _ sdk.PluginProvider = internal.NewPlugin()
}
