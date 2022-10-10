/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sentinel

import (
	"fmt"
	"net/http"

	sentinel "github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/base"
	"github.com/alibaba/sentinel-golang/core/config"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/internal/httputils"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/kit/logger"
)

type middlewareMetadata struct {
	AppName string `json:"appName"`
	// LogConfig
	LogDir string `json:"logDir"`
	// Rules
	FlowRules           string `yaml:"flowRules"`
	CircuitBreakerRules string `yaml:"circuitBreakerRules"`
	HotSpotParamRules   string `yaml:"hotSpotParamRules"`
	IsolationRules      string `yaml:"isolationRules"`
	SystemRules         string `yaml:"systemRules"`
}

// NewMiddleware returns a new sentinel middleware.
func NewMiddleware(logger logger.Logger) middleware.Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an sentinel middleware.
type Middleware struct {
	logger logger.Logger
}

// GetHandler returns the HTTP handler provided by sentinel middleware.
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(next http.Handler) http.Handler, error) {
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

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resourceName := r.Method + ":" + r.URL.Path
			entry, err := sentinel.Entry(
				resourceName,
				sentinel.WithResourceType(base.ResTypeWeb),
				sentinel.WithTrafficType(base.Inbound),
			)
			if err != nil {
				httputils.RespondWithError(w, http.StatusTooManyRequests)
				return
			}
			defer entry.Exit()

			next.ServeHTTP(w, r)
		})
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

	conf.Sentinel.Log.Logger = &loggerAdaptor{m.logger}

	return conf
}

func getNativeMetadata(metadata middleware.Metadata) (*middlewareMetadata, error) {
	var md middlewareMetadata
	err := mdutils.DecodeMetadata(metadata.Properties, &md)
	if err != nil {
		return nil, err
	}
	return &md, nil
}
