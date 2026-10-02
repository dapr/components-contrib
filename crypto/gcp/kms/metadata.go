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

package kms

import (
	"errors"
	"fmt"
	"strings"
	"time"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	kitmd "github.com/dapr/kit/metadata"
)

const defaultRequestTimeout = 30 * time.Second

type kmsMetadata struct {
	// Ignored by the metadata parser because they are included in the built-in "gcp" authentication profile
	Type                string `json:"type" mapstructure:"type" mdignore:"true"`
	ProjectID           string `json:"project_id" mapstructure:"projectID" mdignore:"true" mapstructurealiases:"project_id"`
	PrivateKeyID        string `json:"private_key_id" mapstructure:"privateKeyID" mdignore:"true" mapstructurealiases:"private_key_id"`
	PrivateKey          string `json:"private_key" mapstructure:"privateKey" mdignore:"true" mapstructurealiases:"private_key"`
	ClientEmail         string `json:"client_email" mapstructure:"clientEmail" mdignore:"true" mapstructurealiases:"client_email"`
	ClientID            string `json:"client_id" mapstructure:"clientID" mdignore:"true" mapstructurealiases:"client_id"`
	AuthURI             string `json:"auth_uri" mapstructure:"authURI" mdignore:"true" mapstructurealiases:"auth_uri"`
	TokenURI            string `json:"token_uri" mapstructure:"tokenURI" mdignore:"true" mapstructurealiases:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url" mapstructure:"authProviderX509CertURL" mdignore:"true" mapstructurealiases:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url" mapstructure:"clientX509CertURL" mdignore:"true" mapstructurealiases:"client_x509_cert_url"`

	// Location of the key ring, for example "global" or "us-east1".
	Location string `json:"-" mapstructure:"location"`
	// Name of the key ring that contains the keys used by this component.
	KeyRing string `json:"-" mapstructure:"keyRing"`
	// Timeout for network requests.
	RequestTimeout time.Duration `json:"-" mapstructure:"requestTimeout"`
}

func (m *kmsMetadata) InitWithMetadata(meta contribCrypto.Metadata) error {
	*m = kmsMetadata{
		RequestTimeout: defaultRequestTimeout,
	}

	err := kitmd.DecodeMetadata(meta.Properties, m)
	if err != nil {
		return err
	}

	if m.ProjectID == "" {
		return errors.New("property 'projectID' is required")
	}
	if m.Location == "" {
		return errors.New("property 'location' is required")
	}
	if m.KeyRing == "" {
		return errors.New("property 'keyRing' is required")
	}
	if m.RequestTimeout < time.Second {
		return errors.New("property 'requestTimeout' must be at least 1s")
	}

	return nil
}

// Returns true when the component is configured with explicit service account credentials,
// rather than relying on Application Default Credentials.
func (m kmsMetadata) hasExplicitCredentials() bool {
	return m.PrivateKeyID != ""
}

// keyID identifies a key in the key ring, optionally including its version.
type keyID struct {
	Name      string
	Version   string
	raw       string
	malformed bool
}

func newKeyID(val string) keyID {
	name, version, found := strings.Cut(val, "/")
	return keyID{
		Name:      name,
		Version:   version,
		raw:       val,
		malformed: name == "" || (found && version == "") || strings.Contains(version, "/"),
	}
}

func (id keyID) validate(requireVersion bool) error {
	if id.malformed {
		return fmt.Errorf("key %q is invalid: expected 'name' or 'name/version'", id.raw)
	}
	if requireVersion && id.Version == "" {
		return fmt.Errorf("key '%s' does not include a version: operations with asymmetric keys require a key in the format 'name/version'", id.Name)
	}
	return nil
}

// Cacheable returns true if the public key can be cached locally, which is the case for
// keys pinned to a specific (and therefore immutable) version.
func (id keyID) Cacheable() bool {
	return id.Version != ""
}

// Returns the full resource name of the crypto key.
func (m kmsMetadata) cryptoKeyPath(id keyID) string {
	return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", m.ProjectID, m.Location, m.KeyRing, id.Name)
}

// Returns the full resource name of a crypto key version, which asymmetric operations require.
func (m kmsMetadata) cryptoKeyVersionPath(id keyID) (string, error) {
	if err := id.validate(true); err != nil {
		return "", err
	}
	return m.cryptoKeyPath(id) + "/cryptoKeyVersions/" + id.Version, nil
}
