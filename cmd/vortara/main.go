package main

import (
	_ "go.uber.org/automaxprocs" // container-aware GOMAXPROCS (cgroup CPU limits)

	"github.com/rkshvish/vortaraos/cmd/vortara/cmd"
)

func main() {
	cmd.Execute()
}
