package main

import (
	"crypto/rand"
	"encoding/hex"
	"mime"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
)

type httpContext struct {
	types.DefaultHttpContext
	config                     config // Snapshot: a root reconfiguration cannot relabel an in-flight request.
	plugin                     *plugin
	active, dispatched, failed bool
	requestID                  string
	request                    *request
	decision                   *decision
	started, ended             time.Time
	status                     int
	sse                        *sseParser
	ttft                       *float64
}

func (h *httpContext) fail(status uint32) {
	if h.failed {
		return
	}
	h.failed = true
	h.plugin.routeErrors.Increment(1)
	_ = proxywasm.SendHttpResponse(status, [][2]string{{"content-type", "application/json"}, {"cache-control", "no-store"}}, []byte(`{"error":{"code":"adaptive_routing_failed"}}`), -1)
}
func (h *httpContext) OnHttpRequestHeaders(_ int, end bool) types.Action {
	// Strip even on bypassed paths so clients cannot select an internal route.
	for _, key := range []string{routeHeader, decisionHeader} {
		if proxywasm.RemoveHttpRequestHeader(key) != nil {
			h.fail(500)
			return types.HeaderStopIteration
		}
	}
	path, _ := proxywasm.GetHttpRequestHeader(":path")
	authority, _ := proxywasm.GetHttpRequestHeader(":authority")
	if strings.SplitN(path, "?", 2)[0] != h.config.Path || !h.config.matchesHost(authority) {
		return types.HeaderContinue
	}
	h.active = true
	method, _ := proxywasm.GetHttpRequestHeader(":method")
	contentType, _ := proxywasm.GetHttpRequestHeader("content-type")
	media, _, err := mime.ParseMediaType(contentType)
	encoding, _ := proxywasm.GetHttpRequestHeader("content-encoding")
	if method != "POST" || end || err != nil || media != "application/json" || (encoding != "" && encoding != "identity") {
		h.fail(400)
		return types.HeaderStopIteration
	}
	length, _ := proxywasm.GetHttpRequestHeader("content-length")
	if length != "" {
		n, err := strconv.ParseInt(length, 10, 64)
		if err != nil || n < 0 || n > int64(h.config.MaxBodyBytes) {
			h.fail(413)
			return types.HeaderStopIteration
		}
	}
	h.requestID, _ = proxywasm.GetHttpRequestHeader("x-request-id")
	if h.requestID == "" {
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			h.fail(500)
			return types.HeaderStopIteration
		}
		h.requestID = hex.EncodeToString(id[:])
		_ = proxywasm.ReplaceHttpRequestHeader("x-request-id", h.requestID)
	}
	if !identifier.MatchString(h.requestID) {
		h.fail(400)
		return types.HeaderStopIteration
	}
	if proxywasm.SetProperty([]string{"set_decoder_buffer_limit"}, []byte(strconv.Itoa(h.config.MaxBodyBytes))) != nil {
		h.fail(500)
		return types.HeaderStopIteration
	}
	// The eventual body may change length. Envoy supplies the correct framing.
	if proxywasm.RemoveHttpRequestHeader("content-length") != nil {
		h.fail(500)
	}
	return types.HeaderStopIteration
}

func (c config) matchesHost(authority string) bool {
	if len(c.Hosts) == 0 {
		return true
	}
	host := authority
	if value, _, err := net.SplitHostPort(authority); err == nil {
		host = value
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, allowed := range c.Hosts {
		if host == allowed {
			return true
		}
	}
	return false
}
func (h *httpContext) OnHttpRequestBody(size int, end bool) types.Action {
	if !h.active {
		return types.DataContinue
	}
	if h.failed || h.dispatched {
		return types.DataStopIterationAndBuffer
	}
	if size > h.config.MaxBodyBytes {
		h.fail(413)
		return types.DataStopIterationAndBuffer
	}
	if !end {
		return types.DataStopIterationAndBuffer
	}
	body, err := proxywasm.GetHttpRequestBody(0, size)
	if err != nil {
		h.fail(400)
		return types.DataStopIterationAndBuffer
	}
	h.request, err = parseRequest(body, h.config.TenantID, h.requestID)
	if err != nil {
		h.fail(400)
		return types.DataStopIterationAndBuffer
	}
	h.dispatched = true
	_, err = proxywasm.DispatchHttpCall(h.config.ClassifierCluster, h.config.headers("/v1/route", h.config.RouteToken), h.request.envelope, nil, h.config.TimeoutMS, h.onDecision)
	if err != nil {
		h.fail(503)
	}
	return types.DataStopIterationAndBuffer
}
func (h *httpContext) OnHttpRequestTrailers(int) types.Action {
	if !h.active {
		return types.ActionContinue
	}
	h.fail(400)
	return types.ActionPause
}
func (h *httpContext) onDecision(_ int, size int, _ int) {
	if h.failed || h.decision != nil {
		return
	}
	status := callStatus()
	if status != 200 || size > maxDecisionBytes {
		h.fail(503)
		return
	}
	body, err := proxywasm.GetHttpCallResponseBody(0, size)
	if err != nil {
		h.fail(503)
		return
	}
	d, err := parseDecision(body, h.requestID, h.request.model, h.config)
	if err != nil {
		h.fail(502)
		return
	}
	rewritten, err := h.request.rewrite(d.UpstreamModel)
	if err != nil {
		h.fail(500)
		return
	}
	if proxywasm.ReplaceHttpRequestBody(rewritten) != nil || proxywasm.ReplaceHttpRequestHeader(routeHeader, d.RouteID) != nil {
		h.fail(500)
		return
	}
	// Higress clears the route cache on request-header mutation by default.
	// Do not set clear_route_cache=off (the wrapper's DisableReroute option).
	h.decision = d
	h.started = h.plugin.now()
	h.request = nil
	if proxywasm.ResumeHttpRequest() != nil {
		h.fail(500)
	}
}
func (c config) headers(path, token string) [][2]string {
	return [][2]string{{":method", "POST"}, {":path", path}, {":authority", c.ClassifierAuthority}, {"content-type", "application/json"}, {"authorization", "Bearer " + token}}
}
func callStatus() int {
	headers, err := proxywasm.GetHttpCallResponseHeaders()
	if err != nil {
		return 0
	}
	for _, h := range headers {
		if h[0] == ":status" {
			n, _ := strconv.Atoi(h[1])
			return n
		}
	}
	return 0
}
