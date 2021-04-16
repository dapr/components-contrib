package sentinel

import (
	"encoding/json"
	"github.com/alibaba/sentinel-golang/core/circuitbreaker"
	"github.com/alibaba/sentinel-golang/core/flow"
	"github.com/alibaba/sentinel-golang/core/isolation"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFlowRules(t *testing.T) {
	rules := []*flow.Rule{
		{
			Resource:               "some-test",
			Threshold:              100,
			TokenCalculateStrategy: flow.Direct,
			ControlBehavior:        flow.Reject,
		},
	}

	b, _ := json.Marshal(rules)
	t.Logf("%s", b)
	err := loadRules(string(b), newFlowRuleDataSource)
	assert.Nil(t, err)
}

func TestCircuitBreakerRules(t *testing.T) {
	rules := []*circuitbreaker.Rule{
		{
			Resource:         "abc",
			Strategy:         circuitbreaker.ErrorCount,
			RetryTimeoutMs:   3000,
			MinRequestAmount: 10,
			StatIntervalMs:   5000,
			Threshold:        50,
		},
	}

	b, _ := json.Marshal(rules)
	t.Logf("%s", b)
	err := loadRules(string(b), newCircuitBreakerRuleDataSource)
	assert.Nil(t, err)
}

func TestHotspotParamRules(t *testing.T) {
	rules := `
[
	{
		"resource": "abc",
		"metricType": 1,
		"controlBehavior": 0,
		"paramIndex": 1,
		"threshold": 50,
		"burstCount": 0,
		"durationInSec": 1
	}
]
`
	err := loadRules(rules, newHotSpotParamRuleDataSource)
	assert.Nil(t, err)
}

func TestIsolationRules(t *testing.T) {
	rules := []*isolation.Rule{
		{
			Resource:   "abc",
			MetricType: isolation.Concurrency,
			Threshold:  12,
		},
	}

	b, _ := json.Marshal(rules)
	t.Logf("%s", b)
	err := loadRules(string(b), newIsolationRuleDataSource)
	assert.Nil(t, err)
}
