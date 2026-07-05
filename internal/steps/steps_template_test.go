package steps

import (
	"testing"
	"time"
)

func TestEvalAddExpr_TemplateWrapper(t *testing.T) {
	// The documented form {{ now() }} must behave like bare now().
	got := evalAddExpr("{{ now() }}", nil)
	if _, ok := got.(time.Time); !ok {
		t.Fatalf("{{ now() }} = %v (%T), want time.Time", got, got)
	}
	got = evalAddExpr("now()", nil)
	if _, ok := got.(time.Time); !ok {
		t.Fatalf("now() = %v (%T), want time.Time", got, got)
	}
	// Field references inside braces resolve too.
	got = evalAddExpr("{{ tier }}", map[string]interface{}{"tier": "gold"})
	if got != "gold" {
		t.Fatalf("{{ tier }} = %v, want gold", got)
	}
}

func TestEvalAddExpr_TemplateConcat(t *testing.T) {
	data := map[string]interface{}{"first": "Ada", "last": "Lovelace", "n": int64(7)}
	got := evalAddExpr("{{ first }} {{ last }}", data)
	if got != "Ada Lovelace" {
		t.Fatalf("concat = %v, want Ada Lovelace", got)
	}
	got = evalAddExpr("deal-{{ n }}", data)
	if got != "deal-7" {
		t.Fatalf("mixed = %v, want deal-7", got)
	}
	// Whole-string placeholder still preserves the value's type.
	if got := evalAddExpr("{{ n }}", data); got != int64(7) {
		t.Fatalf("typed passthrough = %v (%T), want int64 7", got, got)
	}
}
