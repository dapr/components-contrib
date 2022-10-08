package contenttype

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContentType(t *testing.T) {
	uts := []struct {
		ContentType string
		Result      bool
		Desc        string
		fn          func(contentType string) bool
	}{
		{
			"application/cloudevents+json",
			true,
			"the content-type is cloudevent",
			IsCloudEventContentType,
		},
		{
			"application/json",
			true,
			"the content-type is json",
			IsJSONContentType,
		},
		{
			"application/json;charset=utf-8",
			true,
			"the content-type is json",
			IsJSONContentType,
		},
		{
			"application/octet-stream",
			true,
			"the content-type is binary",
			IsBinaryContentType,
		},
		{
			"application/grpc",
			true,
			"the content-type is grpc",
			IsGRPCContenttype,
		},
		{
			"text/plain",
			true,
			"the content-type is string",
			IsStringContentType,
		},
		{
			"application/json",
			false,
			"the content-type is json, not grpc",
			IsGRPCContenttype,
		},
	}
	for _, ut := range uts {
		assert.Equalf(t, ut.Result, ut.fn(ut.ContentType), ut.Desc)
	}
}
