// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import (
	"fmt"
)

const (
	FirstWrite = "first-write"
	LastWrite  = "last-write"
	Strong     = "strong"
	Eventual   = "eventual"
)

// CheckSetRequestOptions checks if set request options use supported keywords
func CheckSetRequestOptions(req *SetRequest) error {
	if req.Options.Concurrency != "" && req.Options.Concurrency != FirstWrite && req.Options.Concurrency != LastWrite {
		return fmt.Errorf("unrecognized concurrency model '%s'", req.Options.Concurrency)
	}
	if req.Options.Consistency != "" && req.Options.Consistency != Strong && req.Options.Consistency != Eventual {
		return fmt.Errorf("unrecognized consistency model '%s'", req.Options.Consistency)
	}
	return nil
}

// CheckDeleteRequestOptions checks if delete request options use supported keywords
func CheckDeleteRequestOptions(req *DeleteRequest) error {
	if req.Options.Concurrency != "" && req.Options.Concurrency != FirstWrite && req.Options.Concurrency != LastWrite {
		return fmt.Errorf("unrecognized concurrency model '%s'", req.Options.Concurrency)
	}
	if req.Options.Consistency != "" && req.Options.Consistency != Strong && req.Options.Consistency != Eventual {
		return fmt.Errorf("unrecognized consistency model '%s'", req.Options.Consistency)
	}
	return nil
}

// SetWithOptions handles SetRequest with request options
func SetWithOptions(method func(req *SetRequest) error, req *SetRequest) error {
	return method(req)
}

// DeleteWithOptions handles DeleteRequest with options
func DeleteWithOptions(method func(req *DeleteRequest) error, req *DeleteRequest) error {
	return method(req)
}
