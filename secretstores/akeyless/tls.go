/*
Copyright 2026 The Dapr Authors
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

package akeyless

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
)

func createTLSConfig(gatewayTLSCa string) (*tls.Config, error) {
	certBytes, err := base64.StdEncoding.DecodeString(gatewayTLSCa)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64-encoded gateway TLS CA: %w", err)
	}

	block, _ := pem.Decode(certBytes)
	if block == nil {
		return nil, errors.New("failed to decode PEM certificate: invalid PEM format")
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(certBytes) {
		return nil, errors.New("failed to add certificate to cert pool")
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    caCertPool,
	}, nil
}
