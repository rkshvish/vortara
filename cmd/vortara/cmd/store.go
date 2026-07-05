package cmd

import (
	v2config "github.com/rkshvish/vortara/pkg/config/v2"

	"github.com/rkshvish/vortara/internal/state"
)

func openStore(cfg *v2config.PipelineConfig) (state.StateStore, error) {
	return state.Build(v2config.ToStateConfig(cfg.Settings))
}
