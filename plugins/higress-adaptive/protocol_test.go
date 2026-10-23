package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEnvelopeProjectsFactsAndRewritePreservesGenerationFields(t *testing.T) {
	body := []byte(`{"model":"auto","messages":[{"role":"user","content":"你好"}],"temperature":0.2,"stream":true,"tools":[{"type":"function","function":{"name":"weather"}}],"response_format":{"type":"json_object"}}`)
	r, err := parseRequest(body, "tenant", "request-1")
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(r.envelope, &envelope); err != nil {
		t.Fatal(err)
	}
	req := envelope["request"].(map[string]any)
	if req["temperature"] != nil || req["tenant_id"] != "tenant" || req["tools"] == nil || req["response_format"] == nil {
		t.Fatalf("bad facts: %s", r.envelope)
	}
	updated, err := r.rewrite("provider/model")
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]any
	json.Unmarshal(updated, &actual)
	if actual["temperature"] != 0.2 || actual["model"] != "provider/model" || actual["stream"] != true {
		t.Fatalf("lost generation fields: %s", updated)
	}
}

func TestRequestRejectsAmbiguousJSON(t *testing.T) {
	for _, body := range []string{
		`{"model":"auto","model":"expensive","messages":[]}`,
		`{"messages":[{"role":"user","content":"a","content":"b"}]}`,
		`{"messages":[]} {}`,
		`{"messages":[],"temperature":NaN}`,
		`{"messages":[],"metadata":{"x":1,"x":2}}`,
		`{"messages":[],"model":null}`,
	} {
		if _, err := parseRequest([]byte(body), "tenant", "req"); err == nil {
			t.Errorf("accepted %s", body)
		}
	}
}

func TestRequestRejectsUnmodelledRequirements(t *testing.T) {
	for _, field := range []string{"functions", "function_call", "modalities", "audio", "prediction", "vendor_capability"} {
		for _, value := range []string{`null`, `{}`, `["audio"]`} {
			body := []byte(`{"messages":[{"role":"user","content":"hello"}],"` + field + `":` + value + `}`)
			if _, err := parseRequest(body, "tenant", "req"); err == nil {
				t.Errorf("accepted unmodelled requirement %s=%s", field, value)
			}
		}
	}
}

func TestDecisionRequiresCompleteAllowlistedTarget(t *testing.T) {
	cfg := testConfig()
	body := []byte(validDecision)
	if _, err := parseDecision(body, "r1", "auto", cfg); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{`"schema_version":"1"`, `"schema_version":"2"`}, {`"route_id":"route-a"`, `"route_id":"evil"`}, {`"cluster_id":"a"`, `"cluster_id":"b"`}, {`"upstream_model":"alias-a"`, `"upstream_model":"evil"`}, {`"request_id":"r1"`, `"request_id":"r2"`}, {`"decision_id":"00000000-0000-4000-8000-000000000001"`, `"decision_id":"bad"`}} {
		if _, err := parseDecision([]byte(strings.Replace(validDecision, pair[0], pair[1], 1)), "r1", "auto", cfg); err == nil {
			t.Errorf("accepted %v", pair)
		}
	}
	if _, err := parseDecision(body, "r1", "fixed-other", cfg); err == nil {
		t.Fatal("fixed model bypassed")
	}
}

const validDecision = `{"schema_version":"1","request_id":"r1","decision_id":"00000000-0000-4000-8000-000000000001","attempt_id":"initial","model_id":"basic","model_version":"v1","cluster_id":"a","route_id":"route-a","upstream_model":"alias-a","propensity":1,"policy_version":"v1","feature_version":"v1"}`

func testConfig() config {
	return config{TenantID: "tenant", ClassifierCluster: "classifier", ClassifierAuthority: "classifier", RouteToken: "route-token-123456", ObserveToken: "observe-token-123456", Path: "/v1/chat/completions", TimeoutMS: 1000, MaxBodyBytes: 262144, Targets: map[string]target{"route-a": {ModelID: "basic", ClusterID: "a", UpstreamModel: "alias-a"}}}
}
