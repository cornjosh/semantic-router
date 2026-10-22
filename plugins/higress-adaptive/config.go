package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
)

const routeHeader = "x-semantic-route"
const decisionHeader = "x-semantic-decision-id"
const maxDecisionBytes = 65536

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$`)
var routeName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
var uuidPattern = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)
var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var hostname = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)

type target struct {
	ModelID       string `json:"model_id"`
	ClusterID     string `json:"cluster_id"`
	UpstreamModel string `json:"upstream_model"`
}

type config struct {
	TenantID            string            `json:"tenant_id"`
	ClassifierCluster   string            `json:"classifier_cluster"`
	ClassifierAuthority string            `json:"classifier_authority"`
	RouteToken          string            `json:"route_token"`
	ObserveToken        string            `json:"observe_token"`
	RouteTokenEnv       string            `json:"route_token_env,omitempty"`
	ObserveTokenEnv     string            `json:"observe_token_env,omitempty"`
	Path                string            `json:"path"`
	Hosts               []string          `json:"hosts,omitempty"`
	TimeoutMS           uint32            `json:"timeout_ms"`
	MaxBodyBytes        int               `json:"max_body_bytes"`
	Targets             map[string]target `json:"targets"`
}

func parseConfig(data []byte) (config, error) {
	c := config{Path: "/v1/chat/completions", TimeoutMS: 5000, MaxBodyBytes: 262144}
	if len(data) > 65536 || strictJSON(data) != nil {
		return c, errors.New("invalid configuration")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.New("invalid configuration")
	}
	for _, pair := range []struct {
		value *string
		env   string
	}{{&c.RouteToken, c.RouteTokenEnv}, {&c.ObserveToken, c.ObserveTokenEnv}} {
		if pair.env != "" {
			if *pair.value != "" || !environmentName.MatchString(pair.env) {
				return c, errors.New("ambiguous credential configuration")
			}
			*pair.value = os.Getenv(pair.env)
		}
	}
	if len(c.Hosts) > 128 {
		return c, errors.New("invalid hosts")
	}
	for _, host := range c.Hosts {
		if len(host) > 253 || !hostname.MatchString(host) {
			return c, errors.New("invalid hosts")
		}
	}
	if !identifier.MatchString(c.TenantID) || c.ClassifierCluster == "" || len(c.ClassifierCluster) > 256 || c.ClassifierAuthority == "" || len(c.ClassifierAuthority) > 256 || strings.ContainsAny(c.ClassifierAuthority, " /\t\r\n") || !strings.HasPrefix(c.Path, "/") || strings.ContainsAny(c.Path, "?\r\n") || c.TimeoutMS < 1 || c.TimeoutMS > 30000 || c.MaxBodyBytes < 1024 || c.MaxBodyBytes > 1048576 || len(c.Targets) == 0 || len(c.Targets) > 128 {
		return c, errors.New("invalid configuration")
	}
	for _, token := range []string{c.RouteToken, c.ObserveToken} {
		if len(token) < 16 || len(token) > 4096 || strings.ContainsAny(token, " \t\r\n") {
			return c, errors.New("invalid credential")
		}
	}
	for name, t := range c.Targets {
		if !routeName.MatchString(name) || !identifier.MatchString(t.ModelID) || !identifier.MatchString(t.ClusterID) || !identifier.MatchString(t.UpstreamModel) {
			return c, errors.New("invalid target")
		}
	}
	return c, nil
}

// Reject duplicate keys at every depth, trailing values and excessive nesting.
// encoding/json alone silently accepts duplicate object keys.
func strictJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := jsonValue(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func jsonValue(d *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("JSON nesting")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			keys := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				name, ok := k.(string)
				if !ok || keys[name] {
					return errors.New("duplicate key")
				}
				keys[name] = true
				if e = jsonValue(d, depth+1); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e := jsonValue(d, depth+1); e != nil {
					return e
				}
			}
		default:
			return errors.New("unexpected delimiter")
		}
		_, err = d.Token()
		return err
	}
	if n, ok := t.(json.Number); ok {
		if _, err = n.Float64(); err != nil {
			return errors.New("invalid number")
		}
	}
	return nil
}
