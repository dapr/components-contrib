// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sentinel

import (
	"encoding/json"
	"fmt"

	sentinel "github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/base"
	"github.com/alibaba/sentinel-golang/core/config"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

type middlewareMetadata struct {
	AppName string `json:"appName"`
	// LogConfig
	LogDir    string `json:"logDir"`
	LogUsePid bool   `json:"logUsePid,string"`
	// StatConfig
	GlobalStatisticSampleCountTotal uint32 `yaml:"globalStatisticSampleCountTotal"`
	GlobalStatisticIntervalMsTotal  uint32 `yaml:"globalStatisticIntervalMsTotal"`
	MetricStatisticSampleCount      uint32 `yaml:"metricStatisticSampleCount"`
	MetricStatisticIntervalMs       uint32 `yaml:"metricStatisticIntervalMs"`
	// SystemStatConfig
	CollectIntervalMs uint32 `yaml:"collectIntervalMs"`
	UseCacheTime      bool   `yaml:"useCacheTime"`
	// Rules
	FlowRules           string `yaml:"flowRules"`
	CircuitBreakerRules string `yaml:"circuitBreakerRules"`
	HotSpotParamRules   string `yaml:"hotSpotParamRules"`
	IsolationRules      string `yaml:"isolationRules"`
	SystemRules         string `yaml:"systemRules"`
}

// NewSentinelMiddleware returns a new sentinel middleware
func NewSentinelMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an sentinel middleware
type Middleware struct {
	logger logger.Logger
}

// GetHandler returns the HTTP handler provided by sentinel middleware
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	var (
		meta *middlewareMetadata
		err  error
	)

	meta, err = getNativeMetadata(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "error to parse sentinel metadata")
	}

	conf := m.newSentinelConfig(meta)
	err = sentinel.InitWithConfig(conf)
	if err != nil {
		return nil, errors.Wrapf(err, "error to init sentinel with config: %s", conf)
	}

	err = m.loadSentinelRules(meta)
	if err != nil {
		return nil, err
	}

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			resourceName := string(ctx.Method()) + ":" + string(ctx.Path())
			entry, err := sentinel.Entry(
				resourceName,
				sentinel.WithResourceType(base.ResTypeWeb),
				sentinel.WithTrafficType(base.Inbound),
			)

			if err != nil {
				ctx.Error(fasthttp.StatusMessage(fasthttp.StatusTooManyRequests), fasthttp.StatusTooManyRequests)
				return
			}

			defer entry.Exit()
			h(ctx)
		}
	}, nil
}

func (m *Middleware) loadSentinelRules(meta *middlewareMetadata) error {
	if meta.FlowRules != "" {
		err := loadRules(meta.FlowRules, newFlowRuleDataSource)
		if err != nil {
			msg := fmt.Sprintf("fail to load sentinel flow rules: %s", meta.FlowRules)
			return errors.Wrap(err, msg)
		}
	}

	if meta.IsolationRules != "" {
		err := loadRules(meta.IsolationRules, newIsolationRuleDataSource)
		if err != nil {
			msg := fmt.Sprintf("fail to load sentinel isolation rules: %s", meta.IsolationRules)
			return errors.Wrap(err, msg)
		}
	}

	if meta.CircuitBreakerRules != "" {
		err := loadRules(meta.CircuitBreakerRules, newCircuitBreakerRuleDataSource)
		if err != nil {
			msg := fmt.Sprintf("fail to load sentinel circuit breaker rules: %s", meta.CircuitBreakerRules)
			return errors.Wrap(err, msg)
		}
	}

	if meta.HotSpotParamRules != "" {
		err := loadRules(meta.HotSpotParamRules, newHotSpotParamRuleDataSource)
		if err != nil {
			msg := fmt.Sprintf("fail to load sentinel hotspot param rules: %s", meta.HotSpotParamRules)
			return errors.Wrap(err, msg)
		}
	}

	if meta.SystemRules != "" {
		err := loadRules(meta.SystemRules, newSystemRuleDataSource)
		if err != nil {
			msg := fmt.Sprintf("fail to load sentinel system rules: %s", meta.SystemRules)
			return errors.Wrap(err, msg)
		}
	}

	return nil
}

func (m *Middleware) newSentinelConfig(metadata *middlewareMetadata) *config.Entity {
	conf := config.NewDefaultConfig()

	if metadata.AppName != "" {
		conf.Sentinel.App.Name = metadata.AppName
	}

	if metadata.LogDir != "" {
		conf.Sentinel.Log.Dir = metadata.LogDir
	}

	if metadata.LogUsePid {
		conf.Sentinel.Log.UsePid = metadata.LogUsePid
	}

	if metadata.GlobalStatisticSampleCountTotal > 0 {
		conf.Sentinel.Stat.GlobalStatisticSampleCountTotal = metadata.GlobalStatisticSampleCountTotal
	}

	if metadata.GlobalStatisticIntervalMsTotal > 0 {
		conf.Sentinel.Stat.GlobalStatisticIntervalMsTotal = metadata.GlobalStatisticIntervalMsTotal
	}

	if metadata.MetricStatisticSampleCount > 0 {
		conf.Sentinel.Stat.MetricStatisticSampleCount = metadata.MetricStatisticSampleCount
	}

	if metadata.MetricStatisticIntervalMs > 0 {
		conf.Sentinel.Stat.MetricStatisticIntervalMs = metadata.MetricStatisticIntervalMs
	}

	if metadata.CollectIntervalMs > 0 {
		conf.Sentinel.Stat.System.CollectIntervalMs = metadata.CollectIntervalMs
	}

	if metadata.UseCacheTime {
		conf.Sentinel.UseCacheTime = metadata.UseCacheTime
	}

	return conf
}

func getNativeMetadata(metadata middleware.Metadata) (*middlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var md middlewareMetadata
	err = json.Unmarshal(b, &md)
	if err != nil {
		return nil, err
	}

	return &md, nil
}