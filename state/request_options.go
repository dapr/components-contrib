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
