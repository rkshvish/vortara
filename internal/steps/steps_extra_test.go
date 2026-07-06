package steps

import (
	"testing"

	"github.com/rkshvish/vortara/pkg/row"
)

func TestTrimStep(t *testing.T) {
	p, err := New([]TransformStep{{Trim: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	r := row.Row{Data: map[string]interface{}{"a": "  padded  ", "b": int64(3), "c": "ok"}}
	out, _ := p.Apply(r)
	if out.Data["a"] != "padded" || out.Data["b"] != int64(3) || out.Data["c"] != "ok" {
		t.Fatalf("trim all = %v", out.Data)
	}
	// Input row untouched (clone-once contract).
	if r.Data["a"] != "  padded  " {
		t.Fatal("input mutated")
	}

	p, _ = New([]TransformStep{{Trim: []string{"a"}}})
	r = row.Row{Data: map[string]interface{}{"a": " x ", "z": " y "}}
	out, _ = p.Apply(r)
	if out.Data["a"] != "x" || out.Data["z"] != " y " {
		t.Fatalf("trim selective = %v", out.Data)
	}
}

func TestFlattenStep(t *testing.T) {
	p, err := New([]TransformStep{{Flatten: "_"}})
	if err != nil {
		t.Fatal(err)
	}
	r := row.Row{Data: map[string]interface{}{
		"id": int64(1),
		"user": map[string]interface{}{
			"email": "a@x.com",
			"geo":   map[string]interface{}{"country": "IN"},
		},
	}}
	out, _ := p.Apply(r)
	if out.Data["user_email"] != "a@x.com" {
		t.Fatalf("flatten level 1 = %v", out.Data)
	}
	if out.Data["user_geo_country"] != "IN" {
		t.Fatalf("flatten recursive = %v", out.Data)
	}
	if _, exists := out.Data["user"]; exists {
		t.Fatal("nested key should be removed")
	}
	if out.Data["id"] != int64(1) {
		t.Fatal("scalar untouched")
	}
	// Input untouched.
	if _, ok := r.Data["user"].(map[string]interface{}); !ok {
		t.Fatal("input mutated")
	}
}
