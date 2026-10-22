package main

import (
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
)

func main() {}
func init() { proxywasm.SetVMContext(&vmContext{}) }

type vmContext struct{ types.DefaultVMContext }

func (*vmContext) NewPluginContext(uint32) types.PluginContext { return newPlugin() }

type plugin struct {
	types.DefaultPluginContext
	config                                          config
	now                                             func() time.Time
	queue                                           []*queuedObservation
	inflight                                        int
	dropped, delivered, routeErrors, unknownOutcome proxywasm.MetricCounter
}

func newPlugin() *plugin { return &plugin{now: time.Now} }
func (p *plugin) OnPluginStart(int) types.OnPluginStartStatus {
	data, err := proxywasm.GetPluginConfiguration()
	if err != nil {
		return types.OnPluginStartStatusFailed
	}
	c, err := parseConfig(data)
	if err != nil {
		proxywasm.LogError("adaptive: invalid configuration")
		return types.OnPluginStartStatusFailed
	}
	p.config = c
	p.dropped = proxywasm.DefineCounterMetric("adaptive_observations_dropped")
	p.delivered = proxywasm.DefineCounterMetric("adaptive_observations_delivered")
	p.routeErrors = proxywasm.DefineCounterMetric("adaptive_route_errors")
	p.unknownOutcome = proxywasm.DefineCounterMetric("adaptive_observations_unknown_outcome")
	if proxywasm.SetTickPeriodMilliSeconds(100) != nil {
		return types.OnPluginStartStatusFailed
	}
	return types.OnPluginStartStatusOK
}
func (p *plugin) NewHttpContext(uint32) types.HttpContext {
	return &httpContext{plugin: p, config: p.config}
}
