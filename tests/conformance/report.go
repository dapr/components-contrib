package conformance

import (
	"encoding/json"
	"strings"
)

const (
	jsonFormat := "json"
)

type FunctionReport struct {
	FunctionName          string `json:"function_name"`
	Included              bool   `json:"included"`
	Conformant            bool   `json:"conformed"`
	ExecutionDurationInμs int64  `json:"execution_duration_in_μs"`
}

func NewFunctionReport(name string, durationInμs int64, conformant bool) *FunctionReport {
	return &FunctionReport{
		FunctionName:          name,
		Included:              true,
		ExecutionDurationInμs: durationInμs,
		Conformant:            conformant,
	}
}

type ComponentReport struct {
	Conformant    bool                      `json:"conformant"`
	ComponentType string                    `json:"component_type"`
	ComponentName string                    `json:"component_name"`
	Functions     map[string]FunctionReport `json:"functions"`
}

func NewComponentReport(cname, ctype string) *ComponentReport {
	return &ComponentReport{
		ComponentName: cname,
		ComponentType: ctype,
		Functions:     make(map[string]FunctionReport),
	}
}

func (c *ComponentReport) AddFunctionReport(fr *FunctionReport) {
	c.Functions[fr.FunctionName] = *fr
}

func (c *ComponentReport) Render(format string, pretty bool) ([]byte, string, error) {
	conforms := true
	for _, f := range c.Functions {
		if !f.Conformant && f.Included {
			conforms = false
		}
	}
	c.Conformant = conforms

	switch strings.ToLower(format) {
	case jsonFormat:
		b, err := renderReportAsJSON(c, pretty)
		return b, jsonFormat, err
	default:
		b, err := renderReportAsJSON(c, pretty)
		return b, jsonFormat, err
	}
}

func renderReportAsJSON(c *ComponentReport, pretty bool) ([]byte, error) {
	var b []byte
	var err error
	if pretty {
		b, err = json.MarshalIndent(*c, "", "    ")
		if err != nil {
			return nil, err
		}
	} else {
		b, err = json.Marshal(*c)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}
