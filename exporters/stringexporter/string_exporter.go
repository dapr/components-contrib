// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package stringexporter

import (
	"strconv"

	"github.com/dapr/components-contrib/exporters"
	"github.com/dapr/dapr/pkg/logger"
	"go.opencensus.io/trace"
)

// Metadata is the string exporter config
type Metadata struct {
	Buffer *string
}

// NewStringExporter returns a new string exporter instance
func NewStringExporter(logger logger.Logger) *Exporter {
	return &Exporter{logger: logger}
}

// Exporter is an OpenCensus string exporter
type Exporter struct {
	Buffer *string
	logger logger.Logger
}

// ExportSpan exports span content to the buffer
func (se *Exporter) ExportSpan(sd *trace.SpanData) {
	*se.Buffer = strconv.Itoa(int(sd.Status.Code))
}

// Init creates a new string exporter endpoint and reporter
func (se *Exporter) Init(daprID string, hostAddress string, metadata exporters.Metadata) error {
	se.Buffer = metadata.Buffer
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	trace.RegisterExporter(se)
	return nil
}

// Unregister removes the exporter
func (se *Exporter) Unregister() {
	trace.UnregisterExporter(se)
}
