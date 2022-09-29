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

	"github.com/alibaba/sentinel-golang/ext/datasource"
)

type propertyDataSource struct {
	datasource.Base
	rules string
}

func loadRules(rules string, newDatasource func(rules string) (datasource.DataSource, error)) error {
	if rules != "" {
		ds, err := newDatasource(rules)
		if err != nil {
			return err
		}

		err = ds.Initialize()
		if err != nil {
			return err
		}
	}

	return nil
}

func newFlowRuleDataSource(rules string) (datasource.DataSource, error) {
	return newDataSource(rules, datasource.NewFlowRulesHandler(datasource.FlowRuleJsonArrayParser))
}

func newCircuitBreakerRuleDataSource(rules string) (datasource.DataSource, error) {
	return newDataSource(rules, datasource.NewCircuitBreakerRulesHandler(datasource.CircuitBreakerRuleJsonArrayParser))
}

func newHotSpotParamRuleDataSource(rules string) (datasource.DataSource, error) {
	return newDataSource(rules, datasource.NewHotSpotParamRulesHandler(datasource.HotSpotParamRuleJsonArrayParser))
}

func newIsolationRuleDataSource(rules string) (datasource.DataSource, error) {
	return newDataSource(rules, datasource.NewIsolationRulesHandler(datasource.IsolationRuleJsonArrayParser))
}

func newSystemRuleDataSource(rules string) (datasource.DataSource, error) {
	return newDataSource(rules, datasource.NewSystemRulesHandler(datasource.SystemRuleJsonArrayParser))
}

func newDataSource(rules string, handlers ...datasource.PropertyHandler) (datasource.DataSource, error) {
	ds := &propertyDataSource{
		rules: rules,
	}
	for _, h := range handlers {
		ds.AddPropertyHandler(h)
	}

	return ds, nil
}

func (p propertyDataSource) ReadSource() ([]byte, error) {
	return []byte(p.rules), nil
}

func (p propertyDataSource) Initialize() error {
	src, err := p.ReadSource()
	if err != nil {
		err = fmt.Errorf("fail to read source, err: %w", err)

		return err
	}

	return p.Handle(src)
}

func (p propertyDataSource) Close() error {
	// no op
	return nil
}
