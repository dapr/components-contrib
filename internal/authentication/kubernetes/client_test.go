/*
Copyright 2023 The Dapr Authors
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

package kubernetes

import (
	"io"
	"strings"
	"testing"

	"github.com/dapr/kit/logger"
)

func TestGetKubeconfigPath(t *testing.T) {
	log := logger.NewLogger("test")
	log.SetOutput(io.Discard)
	tests := []struct {
		name string
		args []string
		env  string
		want string
	}{
		{name: "return default value", want: defaultKubeConfig},
		{name: "CLI flag -kubeconfig cli.yaml", args: []string{"binary", "-kubeconfig", "cli.yaml"}, want: "cli.yaml"},
		{name: "CLI flag -kubeconfig=cli.yaml", args: []string{"binary", "-kubeconfig=cli.yaml"}, want: "cli.yaml"},
		{name: "CLI flag --kubeconfig cli.yaml", args: []string{"binary", "--kubeconfig", "cli.yaml"}, want: "cli.yaml"},
		{name: "CLI flag --kubeconfig=cli.yaml", args: []string{"binary", "--kubeconfig=cli.yaml"}, want: "cli.yaml"},
		{name: "CLI flag --kubeconfig cli.yaml not as last", args: []string{"binary", "--kubeconfig", "cli.yaml", "--foo", "example"}, want: "cli.yaml"},
		{name: "CLI flag --kubeconfig=cli.yaml not as last", args: []string{"binary", "--kubeconfig=cli.yaml", "--foo", "example"}, want: "cli.yaml"},
		{name: "CLI flag --kubeconfig with no value as last", args: []string{"binary", "--kubeconfig"}, want: defaultKubeConfig},
		{name: "CLI flag --kubeconfig with no value and more flags", args: []string{"binary", "--kubeconfig", "--foo"}, want: defaultKubeConfig},
		{name: "env var", env: "KUBECONFIG=env.yaml", want: "env.yaml"},
		{name: "CLI flag has priority over env var", args: []string{"binary", "-kubeconfig", "cli.yaml"}, env: "KUBECONFIG=env.yaml", want: "cli.yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.env != "" {
				parts := strings.SplitN(tt.env, "=", 2)
				t.Setenv(parts[0], parts[1])
			}

			args := tt.args
			if args == nil {
				args = []string{}
			}
			if got := GetKubeconfigPath(log, args); got != tt.want {
				t.Errorf("getKubeconfigPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
