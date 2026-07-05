package cmd

import (
	v2config "github.com/rkshvish/vortaraos/pkg/config/v2"

	"github.com/rkshvish/vortaraos/internal/state"
)

func openStore(cfg *v2config.PipelineConfig) (state.StateStore, error) {
	return state.Build(v2config.ToStateConfig(cfg.Settings))
}
