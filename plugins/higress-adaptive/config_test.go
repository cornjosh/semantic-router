package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCredentialsFromVMEnvironmentAndAmbiguityRejected(t *testing.T) {
	value, _ := json.Marshal(testConfig())
	var cfg map[string]any
	json.Unmarshal(value, &cfg)
	delete(cfg, "route_token")
	delete(cfg, "observe_token")
	cfg["route_token_env"] = "ADAPTIVE_TEST_ROUTE"
	cfg["observe_token_env"] = "ADAPTIVE_TEST_OBSERVE"
	t.Setenv("ADAPTIVE_TEST_ROUTE", "secret-route-123456")
	t.Setenv("ADAPTIVE_TEST_OBSERVE", "secret-observe-123456")
	data, _ := json.Marshal(cfg)
	parsed, err := parseConfig(data)
	if err != nil || parsed.RouteToken != "secret-route-123456" || parsed.ObserveToken != "secret-observe-123456" {
		t.Fatalf("environment credential not loaded: %v", err)
	}
	cfg["route_token"] = "other-inline-secret"
	data, _ = json.Marshal(cfg)
	if _, err = parseConfig(data); err == nil {
		t.Fatal("ambiguous credentials accepted")
	}
	delete(cfg, "route_token")
	t.Setenv("ADAPTIVE_TEST_ROUTE", "")
	data, _ = json.Marshal(cfg)
	if _, err = parseConfig(data); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("missing credentials accepted or leaked")
	}
}
