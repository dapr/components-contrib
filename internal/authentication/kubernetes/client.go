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

package kubernetes

import (
	"flag"
	"os"
	"path/filepath"

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

func getKubeconfigPath(log logger.Logger) string {
	// Check if the path is set via the CLI flag `-kubeconfig`
	// This is deprecated
	var cliVal string
	fs := flag.NewFlagSet(cliFlagName, flag.ContinueOnError)
	fs.StringVar(&cliVal, cliFlagName, "", "absolute path to the kubeconfig file")
	_ = fs.Parse(os.Args)
	cliFlag := fs.Lookup(cliFlagName)
	if cliFlag != nil && cliFlag.Value.String() != "" {
		log.Warn("Setting kubeconfig using the CLI flag --kubeconfig is deprecated and will be removed in a future version")
		return cliFlag.Value.String()
	}

	// Check if we have the KUBECONFIG env var
	envVal := os.Getenv(envVarName)
	if envVal != "" {
		return envVal
	}

	// Return the default value
	home := homedir.HomeDir()
	if home != "" {
		return filepath.Join(home, ".kube", "config")
	}
	return ""
}

// GetKubeClient returns a kubernetes client.
func GetKubeClient(log logger.Logger) (*kubernetes.Clientset, error) {
	conf, err := rest.InClusterConfig()
	if err != nil {
		conf, err = clientcmd.BuildConfigFromFlags("", getKubeconfigPath(log))
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
