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

package bearer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const healthz = "/v1.0/healthz"

func TestMatcher(t *testing.T) {
	tests := []struct {
		label        string
		matcherType  string
		allowlist    string
		path         string
		expectBypass bool
		expectErr    bool
	}{
		{
			label:        "normal exact case in allowlist",
			matcherType:  matchTypeExact,
			allowlist:    defaultAllowlist,
			path:         healthz,
			expectBypass: true,
			expectErr:    false,
		},
		{
			label:        "normal exact case multi path in allowlist",
			matcherType:  matchTypeExact,
			allowlist:    defaultAllowlist + allowlistSeparator + "/another",
			path:         "/another",
			expectBypass: true,
			expectErr:    false,
		},
		{
			label:        "normal exact case not in allowlist",
			matcherType:  matchTypeExact,
			allowlist:    defaultAllowlist,
			path:         "/notbypass",
			expectBypass: false,
			expectErr:    false,
		},
		{
			label:        "invalid white list type",
			matcherType:  "invalid",
			allowlist:    defaultAllowlist,
			path:         "/notbypass",
			expectBypass: false,
			expectErr:    true,
		},
		{
			label:        "normal exact case no allowlist",
			matcherType:  matchTypeExact,
			allowlist:    "",
			path:         healthz,
			expectBypass: false,
			expectErr:    false,
		},
		{
			label:       "invalid regex case",
			matcherType: matchTypeRegex,
			allowlist:   string([]byte{255}),
			path:        healthz,
			expectErr:   true,
		},
		{
			label:        "normal regex case end with '/healthz'",
			matcherType:  matchTypeRegex,
			allowlist:    "/healthz$",
			path:         healthz,
			expectBypass: true,
			expectErr:    false,
		},
		{
			label:        "normal exact case no allowlist",
			matcherType:  matchTypeRegex,
			allowlist:    "",
			path:         healthz,
			expectBypass: false,
			expectErr:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			matcher, err := NewMatcher(test.matcherType, test.allowlist)
			if test.expectErr {
				assert.NotNil(t, err)
				return
			}

			assert.Nil(t, err)
			bypass := matcher.Match(test.path)
			assert.Equal(t, test.expectBypass, bypass)
		})
	}
}
