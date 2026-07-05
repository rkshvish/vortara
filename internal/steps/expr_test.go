package steps

import "testing"

func TestParseExpr_StringEQ(t *testing.T) {
	if _, err := parseExpr("status == 'won'"); err != nil {
		t.Fatalf("parseExpr() error = %v", err)
	}
}

func TestParseExpr_NumberGT(t *testing.T) {
	if _, err := parseExpr("revenue > 10000"); err != nil {
		t.Fatalf("parseExpr() error = %v", err)
	}
}

func TestParseExpr_Complex(t *testing.T) {
	expr, err := parseExpr("status == 'won' AND (revenue > 10000 OR tier == 'enterprise')")
	if err != nil {
		t.Fatalf("parseExpr() error = %v", err)
	}
	if !evalExpr(expr, map[string]interface{}{"status": "won", "revenue": 20000, "tier": "smb"}) {
		t.Fatal("evalExpr() = false, want true")
	}
	if evalExpr(expr, map[string]interface{}{"status": "lost", "revenue": 20000, "tier": "enterprise"}) {
		t.Fatal("evalExpr() = true, want false")
	}
}

func TestEvalExpr_MissingField(t *testing.T) {
	expr, err := parseExpr("status == 'won'")
	if err != nil {
		t.Fatalf("parseExpr() error = %v", err)
	}
	if evalExpr(expr, map[string]interface{}{}) {
		t.Fatal("evalExpr() = true, want false")
	}
}

func TestEvalExpr_TypeMismatch(t *testing.T) {
	expr, err := parseExpr("revenue > 10000")
	if err != nil {
		t.Fatalf("parseExpr() error = %v", err)
	}
	if evalExpr(expr, map[string]interface{}{"revenue": "oops"}) {
		t.Fatal("evalExpr() = true, want false")
	}
}

func TestParseExpr_Invalid(t *testing.T) {
	if _, err := parseExpr("status =="); err == nil {
		t.Fatal("parseExpr() error = nil, want error")
	}
	if _, err := parseExpr("AND status"); err == nil {
		t.Fatal("parseExpr() error = nil, want error")
	}
}
