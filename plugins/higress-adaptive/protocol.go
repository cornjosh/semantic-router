package main

import (
	"encoding/json"
	"errors"
	"math"
)

type request struct {
	fields   map[string]json.RawMessage
	envelope []byte
	model    string
	stream   bool
}

func parseRequest(body []byte, tenant, requestID string) (*request, error) {
	if err := strictJSON(body); err != nil {
		return nil, err
	}
	r := &request{model: "auto"}
	if err := json.Unmarshal(body, &r.fields); err != nil || r.fields == nil {
		return nil, errors.New("expected object")
	}
	// Only pass fields whose routing/capacity semantics are understood. Otherwise
	// a new or legacy capability requirement could bypass classifier constraints.
	// In particular, functions, output audio and prediction inputs are not yet
	// represented by the routing contract. Reject even null rather than silently
	// changing protocol semantics between providers.
	for key := range r.fields {
		switch key {
		case "model", "messages", "tools", "tool_choice", "response_format", "max_tokens", "max_completion_tokens", "stream":
		case "temperature", "top_p", "frequency_penalty", "presence_penalty", "stop", "seed", "logit_bias", "logprobs", "top_logprobs", "n", "stream_options", "user", "metadata", "store", "service_tier", "reasoning_effort", "parallel_tool_calls", "safety_identifier", "prompt_cache_key", "prompt_cache_retention":
		default:
			return nil, errors.New("unsupported generation field")
		}
	}
	if v, ok := r.fields["model"]; ok {
		if string(v) == "null" || json.Unmarshal(v, &r.model) != nil || r.model == "" {
			return nil, errors.New("invalid model")
		}
	}
	if v, ok := r.fields["stream"]; ok {
		if string(v) == "null" || json.Unmarshal(v, &r.stream) != nil {
			return nil, errors.New("invalid stream")
		}
	}
	var messages []json.RawMessage
	if json.Unmarshal(r.fields["messages"], &messages) != nil || len(messages) < 1 || len(messages) > 128 {
		return nil, errors.New("invalid messages")
	}
	facts := map[string]any{"tenant_id": tenant, "request_id": requestID, "model": r.model}
	for _, key := range []string{"messages", "tools", "tool_choice", "response_format", "max_tokens", "max_completion_tokens", "stream"} {
		if v, ok := r.fields[key]; ok {
			facts[key] = v
		}
	}
	var err error
	r.envelope, err = json.Marshal(map[string]any{"schema_version": "1", "request": facts})
	return r, err
}
func (r *request) rewrite(model string) ([]byte, error) {
	v, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	r.fields["model"] = v
	return json.Marshal(r.fields)
}

type decision struct {
	SchemaVersion  string  `json:"schema_version"`
	DecisionID     string  `json:"decision_id"`
	AttemptID      string  `json:"attempt_id"`
	RequestID      string  `json:"request_id"`
	ModelID        string  `json:"model_id"`
	ModelVersion   string  `json:"model_version"`
	ClusterID      string  `json:"cluster_id"`
	RouteID        string  `json:"route_id"`
	UpstreamModel  string  `json:"upstream_model"`
	PolicyVersion  string  `json:"policy_version"`
	FeatureVersion string  `json:"feature_version"`
	Propensity     float64 `json:"propensity"`
}

func parseDecision(body []byte, requestID, model string, c config) (*decision, error) {
	if len(body) > maxDecisionBytes || strictJSON(body) != nil {
		return nil, errors.New("invalid decision")
	}
	d := &decision{}
	if json.Unmarshal(body, d) != nil || d.SchemaVersion != "1" || d.RequestID != requestID || !uuidPattern.MatchString(d.DecisionID) || !identifier.MatchString(d.AttemptID) || !identifier.MatchString(d.ModelVersion) || !identifier.MatchString(d.PolicyVersion) || !identifier.MatchString(d.FeatureVersion) || math.IsNaN(d.Propensity) || d.Propensity <= 0 || d.Propensity > 1 {
		return nil, errors.New("invalid decision")
	}
	t, ok := c.Targets[d.RouteID]
	if !ok || t.ModelID != d.ModelID || t.ClusterID != d.ClusterID || t.UpstreamModel != d.UpstreamModel || (model != "auto" && model != d.ModelID) {
		return nil, errors.New("invalid target")
	}
	return d, nil
}
