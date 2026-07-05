package router

import (
	"fmt"

	"github.com/rkshvish/vortara/internal/steps"
	v2 "github.com/rkshvish/vortara/pkg/config/v2"
	"github.com/rkshvish/vortara/pkg/row"
)

// Route represents a compiled destination route.
type Route struct {
	Index     int
	condition steps.Expr
}

// Router evaluates when: conditions and returns matching destination indices.
type Router struct {
	routes []Route
}

// New compiles destination when: expressions.
func New(dests []v2.DestinationConfig) (*Router, error) {
	routes := make([]Route, len(dests))
	for i, d := range dests {
		routes[i].Index = i
		if d.When == "" {
			continue
		}
		expr, err := steps.ParseExpr(d.When)
		if err != nil {
			return nil, fmt.Errorf("destinations[%d].when: %w", i, err)
		}
		routes[i].condition = expr
	}
	return &Router{routes: routes}, nil
}

// Route returns the indices of destinations that should receive the row.
func (r *Router) Route(row row.Row) []int {
	if r == nil || len(r.routes) == 0 {
		return []int{}
	}
	out := make([]int, 0, len(r.routes))
	for _, route := range r.routes {
		if route.condition == nil || steps.EvalExpr(route.condition, row.Data) {
			out = append(out, route.Index)
		}
	}
	return out
}
