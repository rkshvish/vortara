package pipeline

import "testing"

func TestResolveStr_ExplicitVar(t *testing.T) {
	t.Setenv("MY_VAR", "hello")
	if got := resolveStr("${MY_VAR}", "source", "url"); got != "hello" {
		t.Fatalf("resolveStr() = %q, want hello", got)
	}
}

func TestResolveStr_AutoMapped(t *testing.T) {
	t.Setenv("VORTARA__SOURCE__URL", "postgres://example")
	if got := resolveStr("literal", "source", "url"); got != "postgres://example" {
		t.Fatalf("resolveStr() = %q, want postgres://example", got)
	}
}

func TestResolveStr_LiteralWins(t *testing.T) {
	if got := resolveStr("literal", "source", "url"); got != "literal" {
		t.Fatalf("resolveStr() = %q, want literal", got)
	}
}

func TestResolveStr_Priority(t *testing.T) {
	t.Setenv("MY_VAR", "explicit")
	t.Setenv("VORTARA__SOURCE__URL", "auto")
	if got := resolveStr("${MY_VAR}", "source", "url"); got != "explicit" {
		t.Fatalf("resolveStr() = %q, want explicit", got)
	}
}

func TestResolveList_EnvOverride(t *testing.T) {
	t.Setenv("VORTARA__SOURCE__EXCLUDE", "ssn, credit_card")
	got := resolveList([]string{"a", "b"}, "source", "exclude")
	if len(got) != 2 || got[0] != "ssn" || got[1] != "credit_card" {
		t.Fatalf("resolveList() = %#v, want [ssn credit_card]", got)
	}
}

func TestBuildEnvKey(t *testing.T) {
	cases := map[string]string{
		buildEnvKey("source", "url"):                          "VORTARA__SOURCE__URL",
		buildEnvKey("destinations", "0", "auth", "client_id"): "VORTARA__DESTINATIONS__0__AUTH__CLIENT_ID",
		buildEnvKey("source", "group_id"):                     "VORTARA__SOURCE__GROUP_ID",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("buildEnvKey() = %q, want %q", got, want)
		}
	}
}

func TestApplyEnv_DestinationIndexed(t *testing.T) {
	t.Setenv("VORTARA__DESTINATIONS__0__URL", "https://sf.com")
	cfg := &PipelineConfig{Destinations: []DestinationConfig{{Type: "salesforce"}}}
	applyEnv(cfg)
	if got := cfg.Destinations[0].URL; got != "https://sf.com" {
		t.Fatalf("cfg.Destinations[0].URL = %q, want https://sf.com", got)
	}
}

func TestApplyEnv_AuthResolved(t *testing.T) {
	t.Setenv("VORTARA__DESTINATIONS__0__AUTH__TOKEN", "mytoken")
	cfg := &PipelineConfig{Destinations: []DestinationConfig{{Type: "salesforce", Auth: &AuthConfig{Type: "bearer"}}}}
	applyEnv(cfg)
	if got := cfg.Destinations[0].Auth.Token; got != "mytoken" {
		t.Fatalf("cfg.Destinations[0].Auth.Token = %q, want mytoken", got)
	}
}
