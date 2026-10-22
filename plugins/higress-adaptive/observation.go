package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"mime"
	"strconv"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
)

func (h *httpContext) OnHttpResponseHeaders(_ int, end bool) types.Action {
	if h.decision == nil {
		return types.HeaderContinue
	}
	status, _ := proxywasm.GetHttpResponseHeader(":status")
	h.status, _ = strconv.Atoi(status)
	_ = proxywasm.ReplaceHttpResponseHeader(decisionHeader, h.decision.DecisionID)
	contentType, _ := proxywasm.GetHttpResponseHeader("content-type")
	media, _, _ := mime.ParseMediaType(contentType)
	encoding, _ := proxywasm.GetHttpResponseHeader("content-encoding")
	if h.status >= 200 && h.status < 300 && media == "text/event-stream" && (encoding == "" || encoding == "identity") {
		h.sse = newSSEParser(65536)
	}
	if end {
		h.ended = h.plugin.now()
	}
	return types.HeaderContinue
}
func (h *httpContext) OnHttpResponseBody(size int, end bool) types.Action {
	if h.decision == nil {
		return types.DataContinue
	}
	if h.sse != nil && !h.sse.found && !h.sse.disabled {
		// Read in bounded pieces, without buffering the downstream stream.
		for offset := 0; offset < size; {
			n := size - offset
			if n > 8192 {
				n = 8192
			}
			part, err := proxywasm.GetHttpResponseBody(offset, n)
			if err != nil {
				h.sse.disabled = true
				break
			}
			if h.sse.feed(part) {
				value := float64(h.plugin.now().Sub(h.started)) / float64(time.Millisecond)
				if value >= 0 {
					h.ttft = &value
				}
				break
			}
			if h.sse.disabled {
				break
			}
			offset += n
		}
	}
	if end {
		h.ended = h.plugin.now()
	}
	return types.DataContinue
}
func (h *httpContext) OnHttpResponseTrailers(int) types.Action {
	if h.decision != nil {
		h.ended = h.plugin.now()
	}
	return types.ActionContinue
}

// Envoy 1.36 CoreResponseFlag bit positions, pinned to the Higress 2.2.5 runtime.
// Unknown/incomplete outcomes are omitted; a disconnect is never quality feedback.
func (h *httpContext) outcome() string {
	raw, _ := proxywasm.GetProperty([]string{"response", "flags"})
	var flags uint64
	if len(raw) == 8 {
		flags = binary.LittleEndian.Uint64(raw)
	}
	if flags&((1<<14)|(1<<28)) != 0 {
		return "client_cancel"
	}
	if flags&((1<<2)|(1<<16)|(1<<19)|(1<<22)) != 0 {
		return "timeout"
	}
	if flags&((1<<1)|(1<<4)|(1<<5)|(1<<6)|(1<<7)|(1<<15)|(1<<23)|(1<<26)) != 0 {
		return "upstream_error"
	}
	if !h.ended.IsZero() && flags == 0 {
		if h.status >= 200 && h.status < 300 {
			return "success"
		}
		if h.status >= 500 || h.status == 429 {
			return "upstream_error"
		}
	}
	return ""
}
func (h *httpContext) OnHttpStreamDone() {
	if h.decision == nil {
		return
	}
	d := h.decision
	h.decision = nil // At most one terminal event from this context.
	outcome := h.outcome()
	if outcome == "" {
		h.plugin.unknownOutcome.Increment(1)
		return
	}
	end := h.ended
	if end.IsZero() {
		end = h.plugin.now()
	}
	latency := float64(end.Sub(h.started)) / float64(time.Millisecond)
	if latency < 0 || latency > 86400000 || (h.ttft != nil && (*h.ttft > latency || *h.ttft > 86400000)) {
		h.plugin.dropped.Increment(1)
		return
	}
	id := sha256.Sum256([]byte(d.DecisionID + ":" + d.AttemptID))
	event := map[string]any{"event_id": hex.EncodeToString(id[:]), "decision_id": d.DecisionID, "attempt_id": d.AttemptID, "cluster_id": d.ClusterID, "observed_at": end.UTC().Format(time.RFC3339Nano), "outcome": outcome, "latency_ms": latency, "ttft_ms": h.ttft}
	body, err := json.Marshal(map[string]any{"schema_version": "1", "tenant_id": h.config.TenantID, "event": event})
	if err != nil {
		h.plugin.dropped.Increment(1)
		return
	}
	h.plugin.enqueue(&queuedObservation{body: body, config: h.config, expires: h.plugin.now().Add(30 * time.Second)})
}

type queuedObservation struct {
	config        config
	body          []byte
	tries         int
	next, expires time.Time
}

func (p *plugin) enqueue(item *queuedObservation) {
	if len(p.queue)+p.inflight >= 1024 {
		p.dropped.Increment(1)
		return
	}
	p.queue = append(p.queue, item)
}
func (p *plugin) OnTick() {
	// Dispatch in the ROOT context. Callbacks registered by a deleted HTTP
	// context are discarded by the SDK, even after SetEffectiveContext(root).
	n := len(p.queue)
	for i := 0; i < n && p.inflight < 16; i++ {
		item := p.queue[0]
		p.queue = p.queue[1:]
		if !p.now().Before(item.expires) {
			p.dropped.Increment(1)
			continue
		}
		if p.now().Before(item.next) {
			p.queue = append(p.queue, item)
			continue
		}
		p.inflight++
		item.tries++
		_, err := proxywasm.DispatchHttpCall(item.config.ClassifierCluster, item.config.headers("/v1/observations", item.config.ObserveToken), item.body, nil, item.config.TimeoutMS, func(_ int, size int, _ int) {
			p.inflight--
			status := callStatus()
			if status == 200 && size <= 4096 {
				body, err := proxywasm.GetHttpCallResponseBody(0, size)
				var ack struct {
					Version string `json:"schema_version"`
					Applied *bool  `json:"applied"`
				}
				if err == nil && strictJSON(body) == nil && json.Unmarshal(body, &ack) == nil && ack.Version == "1" && ack.Applied != nil {
					p.delivered.Increment(1)
					return
				}
			}
			p.retry(item, status)
		})
		if err != nil {
			p.inflight--
			p.retry(item, 0)
		}
	}
}
func (p *plugin) retry(item *queuedObservation, status int) {
	if item.tries >= 3 || (status >= 400 && status < 500 && status != 429) {
		p.dropped.Increment(1)
		return
	}
	item.next = p.now().Add(time.Duration(100*(1<<item.tries)) * time.Millisecond)
	p.enqueue(item)
}
