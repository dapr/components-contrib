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
	"os"
	"path/filepath"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"github.com/dapr/kit/logger"
)

const (
	cliFlagName = "kubeconfig"
	envVarName  = "KUBECONFIG"
)

var defaultKubeConfig string

func init() {
	if home := homedir.HomeDir(); home != "" {
		defaultKubeConfig = filepath.Join(home, ".kube", "config")
	}
}

func GetKubeconfigPath(log logger.Logger, args []string) string {
	// Check if the path is set via the CLI flag `--kubeconfig`
	// This is deprecated but kept for backwards compatibility
	var cliVal string
	for i, a := range args {
		parts := strings.SplitN(a, "=", 2)
		if parts[0] == "-"+cliFlagName || parts[0] == "--"+cliFlagName {
			// Case: "--kubeconfig=val" or "-kubeconfig=val"
			if len(parts) == 2 {
				cliVal = parts[1]
				break
			} else if len(args) > (i+1) && !strings.HasPrefix(args[i+1], "-") {
				// Case: "--kubeconfig val" or "-kubeconfig val"
				cliVal = args[i+1]
				break
			}
		}
	}
	if cliVal != "" {
		log.Warnf("Setting kubeconfig using the CLI flag '--%s' is deprecated and will be removed in a future version", cliFlagName)
		return cliVal
	}

	// Check if we have the KUBECONFIG env var
	envVal := os.Getenv(envVarName)
	if envVal != "" {
		return envVal
	}

	// Return the default value
	return defaultKubeConfig
}

// GetKubeClient returns a kubernetes client.
func GetKubeClient(kubeconfig string) (*kubernetes.Clientset, error) {
	conf, err := rest.InClusterConfig()
	if err != nil {
		conf, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	clientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, err
	}

	return clientset, nil
}
