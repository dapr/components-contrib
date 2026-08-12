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

package command

import (
	"encoding/json"
	"fmt"

	"github.com/dapr/kit/metadata"
)

// errInvalidDurationFormat is the shared message used by every command binding so that an
// invalid duration value is reported consistently, naming the offending field and the value.
const errInvalidDurationFormat = "invalid value %s for field '%s' (expected a Go duration string, e.g. \"30s\", \"5m\", \"1h30m\", or a plain integer nanoseconds value): %w"

// parseOptionalDuration decodes an optional duration field that was captured as raw JSON so the
// caller can surface a field-scoped error. It returns (nil, nil) when the field is absent.
func parseOptionalDuration(raw *json.RawMessage, field string) (*metadata.Duration, error) {
	if raw == nil {
		return nil, nil
	}
	var d metadata.Duration
	if err := d.UnmarshalJSON(*raw); err != nil {
		return nil, fmt.Errorf(errInvalidDurationFormat, string(*raw), field, err)
	}
	return &d, nil
}
