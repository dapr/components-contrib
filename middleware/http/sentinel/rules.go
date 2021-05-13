// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sentinel

import (
	"github.com/alibaba/sentinel-golang/ext/datasource"
	"github.com/pkg/errors"
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
		err = errors.Errorf("Fail to read source, err: %+v", err)

		return err
	}

	return p.Handle(src)
}

func (p propertyDataSource) Close() error {
	// no op
	return nil
}
