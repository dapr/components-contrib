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

package metadata

import (
	"testing"
	"time"
)

func TestDuration_ToISOString(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			duration: time.Duration(0),
			want:     "P0D",
		},
		{
			duration: 1 * time.Second,
			want:     "PT1S",
		},
		{
			name:     "truncate fractions of seconds",
			duration: 1100 * time.Millisecond,
			want:     "PT1S",
		},
		{
			duration: 24 * time.Hour,
			want:     "P1D",
		},
		{
			duration: 48 * time.Hour,
			want:     "P2D",
		},
		{
			duration: 50 * time.Hour,
			want:     "P2DT2H",
		},
		{
			duration: 50*time.Hour + 20*time.Minute,
			want:     "P2DT2H20M",
		},
		{
			duration: 50*time.Hour + 100*time.Minute,
			want:     "P2DT3H40M",
		},
		{
			duration: 50*time.Hour + 20*time.Minute + 15*time.Second,
			want:     "P2DT2H20M15S",
		},
		{
			duration: 50*time.Hour + 15*time.Second,
			want:     "P2DT2H15S",
		},
		{
			duration: 240*time.Hour + 15*time.Second,
			want:     "P10DT15S",
		},
	}
	for _, tt := range tests {
		name := tt.name
		if name == "" {
			name = tt.want
		}
		t.Run(name, func(t *testing.T) {
			d := Duration{
				Duration: tt.duration,
			}
			if got := d.ToISOString(); got != tt.want {
				t.Errorf("Duration.ToISOString() = %v, want %v", got, tt.want)
			}
		})
	}
}
