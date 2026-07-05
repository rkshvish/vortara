package router

import (
	"testing"
	"time"

	v2 "github.com/rkshvish/vortaraos/pkg/config/v2"
	"github.com/rkshvish/vortaraos/pkg/row"
)

func TestRouter_NoWhen_AlwaysReceives(t *testing.T) {
	rt, err := New([]v2.DestinationConfig{{Type: "restapi"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := rt.Route(row.NewRow("src", "pipe", "pk", map[string]interface{}{"tier": "x"}, time.Now()))
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("Route() = %v, want [0]", got)
	}
}

func TestRouter_When_True(t *testing.T) {
	rt, err := New([]v2.DestinationConfig{{Type: "restapi", When: "tier == 'enterprise'"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := rt.Route(row.NewRow("src", "pipe", "pk", map[string]interface{}{"tier": "enterprise"}, time.Now()))
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("Route() = %v, want [0]", got)
	}
}

func TestRouter_When_False(t *testing.T) {
	rt, err := New([]v2.DestinationConfig{{Type: "restapi", When: "tier == 'enterprise'"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := rt.Route(row.NewRow("src", "pipe", "pk", map[string]interface{}{"tier": "smb"}, time.Now()))
	if len(got) != 0 {
		t.Fatalf("Route() = %v, want []", got)
	}
}

func TestRouter_MultipleDestinations_FanOut(t *testing.T) {
	rt, err := New([]v2.DestinationConfig{
		{Type: "restapi"},
		{Type: "restapi", When: "tier == 'enterprise'"},
		{Type: "restapi", When: "tier == 'smb'"},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got := rt.Route(row.NewRow("src", "pipe", "pk", map[string]interface{}{"tier": "enterprise"}, time.Now()))
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("enterprise Route() = %v, want [0 1]", got)
	}
	got = rt.Route(row.NewRow("src", "pipe", "pk", map[string]interface{}{"tier": "smb"}, time.Now()))
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("smb Route() = %v, want [0 2]", got)
	}
}

func TestRouter_InvalidWhen_Error(t *testing.T) {
	if _, err := New([]v2.DestinationConfig{{Type: "restapi", When: "invalid =="}}); err == nil {
		t.Fatal("New() error = nil, want error")
	}
}

func TestRouter_EmptyDestinations(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := rt.Route(row.NewRow("src", "pipe", "pk", map[string]interface{}{}, time.Now())); len(got) != 0 {
		t.Fatalf("Route() = %v, want []", got)
	}
}
