package sls

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
)

/**
 * test the metadata in the yaml file
 */
func TestSlsLogstorageMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"Endpoint":        "ENDPOINT",
		"AccessKeyID":     "ACCESSKEYID",
		"AccessKeySecret": "ACCESSKEYSECRET",
	}
	aliCloudSlsLogstorage := AliCloudSlsLogstorage{}

	meta, err := aliCloudSlsLogstorage.parseMeta(m)

	assert.Nil(t, err)
	assert.Equal(t, "ENDPOINT", meta.Endpoint)
	assert.Equal(t, "ACCESSKEYID", meta.AccessKeyID)
	assert.Equal(t, "ACCESSKEYSECRET", meta.AccessKeySecret)
}

/*
 * test the log content
 */
func TestParseLog(t *testing.T) {
	aliCloudSlsLogstorage := AliCloudSlsLogstorage{}
	d, _ := json.Marshal(map[string]string{
		"log1": "LOG1",
		"log2": "LOG2",
	})
	log := bindings.InvokeRequest{
		Data: d,
		Metadata: map[string]string{
			"project":  "PROJECT",
			"logstore": "LOGSTORE",
			"topic":    "TOPIC",
			"source":   "SOURCE",
		},
	}
	parseLog, _ := aliCloudSlsLogstorage.parseLog(&log)
	for _, v := range parseLog.Contents {
		switch *v.Key {
		case "log1":
			assert.Equal(t, "LOG1", *v.Value)
		case "log2":
			assert.Equal(t, "LOG2", *v.Value)
		}
	}
}
