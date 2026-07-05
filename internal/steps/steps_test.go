package steps

import (
	"testing"
	"time"

	v2 "github.com/rkshvish/vortaraos/pkg/config/v2"
	"github.com/rkshvish/vortaraos/pkg/row"
)

func TestProcessor_Filter_Pass(t *testing.T) {
	p, err := New([]v2.TransformStep{{Filter: "status == 'won'"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "won"}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	if out.Data["status"] != "won" {
		t.Fatalf("Apply() status = %v, want won", out.Data["status"])
	}
}

func TestProcessor_Filter_Drop(t *testing.T) {
	p, err := New([]v2.TransformStep{{Filter: "status == 'won'"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "lost"}, time.Now()))
	if ok {
		t.Fatal("Apply() ok = true, want false")
	}
}

func TestProcessor_Filter_AND(t *testing.T) {
	p, err := New([]v2.TransformStep{{Filter: "status == 'won' AND revenue > 10000"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "won", "revenue": 20000}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	_, ok = p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "won", "revenue": 5000}, time.Now()))
	if ok {
		t.Fatal("Apply() ok = true, want false")
	}
}

func TestProcessor_Filter_OR(t *testing.T) {
	p, err := New([]v2.TransformStep{{Filter: "status == 'won' OR revenue > 50000"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "won", "revenue": 1}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	_, ok = p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "lost", "revenue": 10}, time.Now()))
	if ok {
		t.Fatal("Apply() ok = true, want false")
	}
}

func TestProcessor_Filter_NOT(t *testing.T) {
	p, err := New([]v2.TransformStep{{Filter: "NOT status == 'draft'"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "won"}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	_, ok = p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"status": "draft"}, time.Now()))
	if ok {
		t.Fatal("Apply() ok = true, want false")
	}
}

func TestProcessor_Filter_Contains(t *testing.T) {
	p, err := New([]v2.TransformStep{{Filter: "contains(email, '@company.com')"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"email": "john@company.com"}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	_, ok = p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"email": "john@other.com"}, time.Now()))
	if ok {
		t.Fatal("Apply() ok = true, want false")
	}
}

func TestProcessor_Rename(t *testing.T) {
	p, err := New([]v2.TransformStep{{Rename: map[string]string{"deal_name": "Name", "revenue": "Amount"}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"deal_name": "foo", "revenue": 100}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	if _, ok := out.Data["deal_name"]; ok {
		t.Fatal("deal_name still present")
	}
	if out.Data["Name"] != "foo" || out.Data["Amount"] != 100 {
		t.Fatalf("rename output = %#v", out.Data)
	}
}

func TestProcessor_Add_Literal(t *testing.T) {
	p, err := New([]v2.TransformStep{{Add: map[string]string{"source_system": "'postgres'"}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{}, time.Now()))
	if !ok || out.Data["source_system"] != "postgres" {
		t.Fatalf("add literal output = %#v, ok=%v", out.Data, ok)
	}
}

func TestProcessor_Add_Now(t *testing.T) {
	p, err := New([]v2.TransformStep{{Add: map[string]string{"synced_at": "now()"}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	ts, ok := out.Data["synced_at"].(time.Time)
	if !ok {
		t.Fatalf("synced_at type = %T, want time.Time", out.Data["synced_at"])
	}
	if time.Since(ts) > time.Second {
		t.Fatalf("synced_at too old: %v", ts)
	}
}

func TestProcessor_Add_If(t *testing.T) {
	p, err := New([]v2.TransformStep{{Add: map[string]string{"tier": "if revenue > 100000 then 'enterprise' else 'smb'"}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, _ := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"revenue": 200000}, time.Now()))
	if out.Data["tier"] != "enterprise" {
		t.Fatalf("tier = %v, want enterprise", out.Data["tier"])
	}
	out, _ = p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"revenue": 5000}, time.Now()))
	if out.Data["tier"] != "smb" {
		t.Fatalf("tier = %v, want smb", out.Data["tier"])
	}
}

func TestProcessor_Add_FieldRef(t *testing.T) {
	p, err := New([]v2.TransformStep{{Add: map[string]string{"copy": "original_field"}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"original_field": "value"}, time.Now()))
	if !ok || out.Data["copy"] != "value" {
		t.Fatalf("add field ref output = %#v, ok=%v", out.Data, ok)
	}
}

func TestProcessor_Drop(t *testing.T) {
	p, err := New([]v2.TransformStep{{Drop: []string{"internal_notes", "debug"}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"internal_notes": "x", "debug": "y", "keep": "z"}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	if _, ok := out.Data["internal_notes"]; ok {
		t.Fatal("internal_notes still present")
	}
	if _, ok := out.Data["debug"]; ok {
		t.Fatal("debug still present")
	}
	if out.Data["keep"] != "z" {
		t.Fatalf("keep = %v, want z", out.Data["keep"])
	}
}

func TestProcessor_Mask(t *testing.T) {
	p, err := New([]v2.TransformStep{{Mask: []string{"ssn", "credit_card"}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	out, ok := p.Apply(row.NewRow("src", "pipe", "pk", map[string]interface{}{"ssn": "123", "credit_card": "456", "name": "Alice"}, time.Now()))
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	if out.Data["ssn"] != "***" || out.Data["credit_card"] != "***" || out.Data["name"] != "Alice" {
		t.Fatalf("mask output = %#v", out.Data)
	}
}

func TestProcessor_ChainedSteps(t *testing.T) {
	orig := row.NewRow("src", "pipe", "pk", map[string]interface{}{
		"status":      "won",
		"deal_name":   "Acme",
		"revenue":     200000,
		"internal":    "secret",
		"credit_card": "1234",
	}, time.Now())

	p, err := New([]v2.TransformStep{
		{Filter: "status == 'won'"},
		{Rename: map[string]string{"deal_name": "Name"}},
		{Add: map[string]string{"tier": "if revenue > 100000 then 'enterprise' else 'smb'"}},
		{Drop: []string{"internal"}},
		{Mask: []string{"credit_card"}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	out, ok := p.Apply(orig)
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	if out.Data["Name"] != "Acme" || out.Data["tier"] != "enterprise" || out.Data["credit_card"] != "***" {
		t.Fatalf("chain output = %#v", out.Data)
	}
	if _, ok := out.Data["internal"]; ok {
		t.Fatal("internal still present")
	}
	if _, ok := orig.Data["deal_name"]; !ok {
		t.Fatal("original row mutated")
	}
}

func TestProcessor_EmptySteps(t *testing.T) {
	p, err := New(nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	in := row.NewRow("src", "pipe", "pk", map[string]interface{}{"a": 1}, time.Now())
	out, ok := p.Apply(in)
	if !ok {
		t.Fatal("Apply() ok = false, want true")
	}
	if out.Data["a"] != 1 {
		t.Fatalf("out.Data = %#v", out.Data)
	}
}
