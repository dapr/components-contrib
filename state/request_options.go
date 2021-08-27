// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
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

// CheckRequestOptions checks if request options use supported keywords.
func CheckRequestOptions(options interface{}) error {
	switch o := options.(type) {
	case SetStateOption:
		if err := validateConsistencyOption(o.Consistency); err != nil {
			return err
		}
		if err := validateConcurrencyOption(o.Concurrency); err != nil {
			return err
		}
	case DeleteStateOption:
		// no support in golang for multiple condition in type switch, so need to check explicitly
		if err := validateConsistencyOption(o.Consistency); err != nil {
			return err
		}
		if err := validateConcurrencyOption(o.Concurrency); err != nil {
			return err
		}
	case GetStateOption:
		if err := validateConsistencyOption(o.Consistency); err != nil {
			return err
		}
	}

	return nil
}

func validateConcurrencyOption(c string) error {
	if c != "" && c != FirstWrite && c != LastWrite {
		return fmt.Errorf("unrecognized concurrency model '%s'", c)
	}

	return nil
}

func validateConsistencyOption(c string) error {
	if c != "" && c != Strong && c != Eventual {
		return fmt.Errorf("unrecognized consistency model '%s'", c)
	}

	return nil
}

// SetWithOptions handles SetRequest with request options.
func SetWithOptions(method func(req *SetRequest) error, req *SetRequest) error {
	return method(req)
}

// DeleteWithOptions handles DeleteRequest with options.
func DeleteWithOptions(method func(req *DeleteRequest) error, req *DeleteRequest) error {
	return method(req)
}
