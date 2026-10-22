package main

import (
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
)

type harness struct {
	host  proxytest.HostEmulator
	root  *plugin
	clock time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{clock: time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)}
	data, _ := json.Marshal(testConfig())
	opt := proxytest.NewEmulatorOption().WithPluginConfiguration(data).WithPluginContext(func(uint32) types.PluginContext {
		h.root = newPlugin()
		h.root.now = func() time.Time { return h.clock }
		return h.root
	})
	var reset func()
	h.host, reset = proxytest.NewHostEmulator(opt)
	t.Cleanup(reset)
	if h.host.StartPlugin() != types.OnPluginStartStatusOK {
		t.Fatal("startup failed")
	}
	return h
}
func (h *harness) request(t *testing.T, body string) uint32 {
	t.Helper()
	id := h.host.InitializeHttpContext()
	a := h.host.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {"content-type", "application/json"}, {"x-request-id", "r1"}, {routeHeader, "evil"}, {decisionHeader, "fake"}}, false)
	if a != types.HeaderStopIteration {
		t.Fatalf("headers not stopped: %v", a)
	}
	for _, header := range h.host.GetCurrentRequestHeaders(id) {
		if header[0] == routeHeader || header[0] == decisionHeader {
			t.Fatal("untrusted internal header kept")
		}
	}
	if a = h.host.CallOnRequestBody(id, []byte(body), true); a != types.DataStopIterationAndBuffer {
		t.Fatalf("body not stopped: %v", a)
	}
	return id
}
func (h *harness) answer(t *testing.T, id uint32, status, body string) {
	t.Helper()
	calls := h.host.GetCalloutAttributesFromContext(id)
	if len(calls) != 1 {
		t.Fatalf("call count %d", len(calls))
	}
	h.host.CallOnHttpCallResponse(calls[0].CalloutID, [][2]string{{":status", status}}, nil, []byte(body))
}

const basicRequest = `{"model":"auto","messages":[{"role":"user","content":"hello"}],"stream":true,"temperature":0.3}`

func TestHostPausesRewritesAndResumes(t *testing.T) {
	h := newHarness(t)
	id := h.request(t, basicRequest)
	call := h.host.GetCalloutAttributesFromContext(id)[0]
	if call.Upstream != "classifier" || !strings.Contains(string(call.Body), `"tenant_id":"tenant"`) {
		t.Fatalf("wrong call: %+v", call)
	}
	h.answer(t, id, "200", validDecision)
	if h.host.GetCurrentHttpStreamAction(id) != types.ActionContinue {
		t.Fatal("not resumed")
	}
	if !strings.Contains(string(h.host.GetCurrentRequestBody(id)), `"model":"alias-a"`) {
		t.Fatal("model not rewritten")
	}
	found := false
	for _, v := range h.host.GetCurrentRequestHeaders(id) {
		if v[0] == routeHeader && v[1] == "route-a" {
			found = true
		}
	}
	if !found {
		t.Fatal("route header missing")
	}
}

func TestHostClassifierFailuresFailClosed(t *testing.T) {
	for _, test := range []struct{ status, body string }{{"503", `{}`}, {"", ``}, {"200", `{}`}, {"200", strings.Replace(validDecision, "route-a", "evil", 1)}} {
		t.Run(test.status+test.body, func(t *testing.T) {
			h := newHarness(t)
			id := h.request(t, basicRequest)
			h.answer(t, id, test.status, test.body)
			r := h.host.GetSentLocalResponse(id)
			if r == nil || r.StatusCode < 500 {
				t.Fatal("did not fail closed")
			}
			if h.host.GetCurrentHttpStreamAction(id) == types.ActionContinue {
				t.Fatal("resumed failed request")
			}
		})
	}
}

func TestHostSSEUnchangedAndObservationSurvivesContextDeletion(t *testing.T) {
	h := newHarness(t)
	id := h.request(t, basicRequest)
	h.clock = h.clock.Add(700 * time.Millisecond)
	h.answer(t, id, "200", validDecision)
	h.host.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "text/event-stream"}}, false)
	chunks := []string{": keepalive\n\n", "data: {\"choices\":[{\"delta\":{\"con", "tent\":\"hi\"}}]}\n\n", "data: [DONE]\n\n"}
	for i, chunk := range chunks {
		h.clock = h.clock.Add(10 * time.Millisecond)
		h.host.CallOnResponseBody(id, []byte(chunk), i == len(chunks)-1)
		if string(h.host.GetCurrentResponseBody(id)) != chunk {
			t.Fatal("response bytes changed")
		}
	}
	h.host.CompleteHttpContext(id)
	if len(h.root.queue) != 1 {
		t.Fatalf("terminal duplicates/loss: %d", len(h.root.queue))
	}
	h.host.Tick()
	calls := h.host.GetCalloutAttributesFromContext(proxytest.PluginContextID)
	if len(calls) != 1 {
		t.Fatalf("root callouts %d", len(calls))
	}
	var wire struct {
		Event struct {
			TTFT    float64 `json:"ttft_ms"`
			Latency float64 `json:"latency_ms"`
			Outcome string  `json:"outcome"`
		}
	}
	if err := json.Unmarshal(calls[0].Body, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Event.TTFT != 30 || wire.Event.Latency != 40 || wire.Event.Outcome != "success" {
		t.Fatalf("wrong measurements: %+v %s", wire, calls[0].Body)
	}
	h.host.CallOnHttpCallResponse(calls[0].CalloutID, [][2]string{{":status", "200"}}, nil, []byte(`{"schema_version":"1","applied":true}`))
	if h.root.inflight != 0 || len(h.root.queue) != 0 {
		t.Fatal("root callback lost after HTTP context destruction")
	}
}

func TestHostResetAndQueueRetry(t *testing.T) {
	h := newHarness(t)
	id := h.request(t, basicRequest)
	h.answer(t, id, "200", validDecision)
	flags := make([]byte, 8)
	binary.LittleEndian.PutUint64(flags, 1<<2) // upstream timeout in Envoy 1.36
	h.host.SetProperty([]string{"response", "flags"}, flags)
	h.clock = h.clock.Add(time.Second)
	h.host.CompleteHttpContext(id)
	h.host.Tick()
	calls := h.host.GetCalloutAttributesFromContext(proxytest.PluginContextID)
	if len(calls) != 1 || !strings.Contains(string(calls[0].Body), `"outcome":"timeout"`) {
		t.Fatal("timeout missing")
	}
	body := string(calls[0].Body)
	h.host.CallOnHttpCallResponse(calls[0].CalloutID, [][2]string{{":status", "503"}}, nil, nil)
	h.clock = h.clock.Add(time.Second)
	h.host.Tick()
	calls = h.host.GetCalloutAttributesFromContext(proxytest.PluginContextID)
	if len(calls) != 1 || string(calls[0].Body) != body {
		t.Fatal("retry changed event identity/payload")
	}
}

func TestHostNonstreamTTFTUnknownAndLongAttemptID(t *testing.T) {
	h := newHarness(t)
	id := h.request(t, strings.Replace(basicRequest, `"stream":true`, `"stream":false`, 1))
	h.answer(t, id, "200", strings.Replace(validDecision, `"initial"`, `"`+strings.Repeat("a", 128)+`"`, 1))
	h.host.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "application/json"}}, false)
	h.clock = h.clock.Add(time.Second)
	h.host.CallOnResponseBody(id, []byte(`{"choices":[]}`), true)
	h.host.CompleteHttpContext(id)
	var item struct {
		Event struct {
			ID   string   `json:"event_id"`
			TTFT *float64 `json:"ttft_ms"`
		}
	}
	json.Unmarshal(h.root.queue[0].body, &item)
	if len(item.Event.ID) > 128 || item.Event.TTFT != nil {
		t.Fatalf("invalid event: %+v", item)
	}
}

func TestHostConfigReloadCannotChangeInflightTenantOrDestination(t *testing.T) {
	h := newHarness(t)
	id := h.request(t, basicRequest)
	h.root.config.TenantID = "new-tenant"
	h.root.config.ClassifierCluster = "new-classifier"
	h.root.config.ObserveToken = "new-observer-token"
	h.answer(t, id, "200", validDecision)
	h.host.CallOnResponseHeaders(id, [][2]string{{":status", "200"}}, true)
	h.host.CompleteHttpContext(id)
	h.host.Tick()
	calls := h.host.GetCalloutAttributesFromContext(proxytest.PluginContextID)
	if len(calls) != 1 || calls[0].Upstream != "classifier" || !strings.Contains(string(calls[0].Body), `"tenant_id":"tenant"`) {
		t.Fatalf("inflight config changed: %+v", calls)
	}
}

func TestHostUnknownResetNotSuccessAndQueueBounded(t *testing.T) {
	h := newHarness(t)
	id := h.request(t, basicRequest)
	h.answer(t, id, "200", validDecision)
	h.host.CallOnResponseHeaders(id, [][2]string{{":status", "200"}}, false)
	h.host.CompleteHttpContext(id)
	if len(h.root.queue) != 0 {
		t.Fatal("incomplete stream fabricated success")
	}
	for i := 0; i < 1100; i++ {
		h.root.enqueue(&queuedObservation{expires: h.clock.Add(time.Second)})
	}
	if len(h.root.queue) != 1024 {
		t.Fatal("unbounded queue")
	}
	count, err := h.host.GetCounterMetric("adaptive_observations_dropped")
	if err != nil || count != 76 {
		t.Fatalf("drop metric %d %v", count, err)
	}
}

func TestHostBodyFragmentsDispatchOnceAndOversizeRejected(t *testing.T) {
	h := newHarness(t)
	id := h.host.InitializeHttpContext()
	h.host.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {"content-type", "application/json"}}, false)
	h.host.CallOnRequestBody(id, []byte(basicRequest[:30]), false)
	if len(h.host.GetCalloutAttributesFromContext(id)) != 0 {
		t.Fatal("dispatched partial body")
	}
	h.host.CallOnRequestBody(id, []byte(basicRequest[30:]), true)
	if len(h.host.GetCalloutAttributesFromContext(id)) != 1 {
		t.Fatal("no complete-body dispatch")
	}
	h.host.CallOnRequestBody(id, nil, true)
	if len(h.host.GetCalloutAttributesFromContext(id)) != 1 {
		t.Fatal("duplicate dispatch")
	}
	id = h.host.InitializeHttpContext()
	h.host.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {"content-type", "application/json"}, {"content-length", "999999999"}}, false)
	if response := h.host.GetSentLocalResponse(id); response == nil || response.StatusCode != 413 {
		t.Fatal("oversize not rejected")
	}
}

func TestHostRateLimitIsOperationalFailureAndRetriesAreBounded(t *testing.T) {
	h := newHarness(t)
	id := h.request(t, basicRequest)
	h.answer(t, id, "200", validDecision)
	h.host.CallOnResponseHeaders(id, [][2]string{{":status", "429"}}, true)
	h.host.CompleteHttpContext(id)
	if len(h.root.queue) != 1 || !strings.Contains(string(h.root.queue[0].body), `"outcome":"upstream_error"`) {
		t.Fatal("upstream rate limit not observed")
	}
	for i := 0; i < 3; i++ {
		h.clock = h.clock.Add(time.Second)
		h.host.Tick()
		calls := h.host.GetCalloutAttributesFromContext(proxytest.PluginContextID)
		if len(calls) != 1 {
			t.Fatalf("retry %d: %d calls", i, len(calls))
		}
		h.host.CallOnHttpCallResponse(calls[0].CalloutID, [][2]string{{":status", "503"}}, nil, nil)
	}
	if len(h.root.queue) != 0 || h.root.inflight != 0 {
		t.Fatal("unbounded retries")
	}
}

func TestHostScopeDoesNotClassifyOtherVirtualHosts(t *testing.T) {
	h := newHarness(t)
	h.root.config.Hosts = []string{"llm.example.com"}
	for _, test := range []struct {
		host   string
		action types.Action
	}{{"other.example.com", types.HeaderContinue}, {"LLM.EXAMPLE.COM:8080", types.HeaderStopIteration}} {
		id := h.host.InitializeHttpContext()
		action := h.host.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", test.host}, {"content-type", "application/json"}, {routeHeader, "evil"}}, false)
		if action != test.action {
			t.Fatalf("host %s: action %d", test.host, action)
		}
		for _, header := range h.host.GetCurrentRequestHeaders(id) {
			if header[0] == routeHeader {
				t.Fatal("untrusted route header retained on host bypass")
			}
		}
	}
}
