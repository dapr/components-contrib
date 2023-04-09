/*
Copyright 2022 The Dapr Authors
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

package metadataschema

import (
	"fmt"
)

// ParseBuiltinAuthenticationProfile returns an AuthenticationProfile(s) from a given BuiltinAuthenticationProfile.
func ParseBuiltinAuthenticationProfile(bi BuiltinAuthenticationProfile) ([]AuthenticationProfile, error) {
	switch bi.Name {
	case "azuread":
		profiles := []AuthenticationProfile{
			{
				Title:       "Azure AD: Managed identity",
				Description: "Authenticate using Azure AD and a managed identity.",
				Metadata: mergedMetadata(bi.Metadata,
					Metadata{
						Name:        "azureClientId",
						Description: "Client ID (application ID). Required if the service has multiple identities assigned.",
						Example:     `"c7dd251f-811f-4ba2-a905-acd4d3f8f08b"`,
						Required:    false,
					},
				),
			},
			{
				Title:       "Azure AD: Client credentials",
				Description: "Authenticate using Azure AD with client credentials, also known as \"service principals\".",
				Metadata: mergedMetadata(bi.Metadata,
					Metadata{
						Name:        "azureTenantId",
						Description: "ID of the Azure AD tenant",
						Example:     `"cd4b2887-304c-47e1-b4d5-65447fdd542a"`,
						Required:    true,
					},
					Metadata{
						Name:        "azureClientId",
						Description: "Client ID (application ID)",
						Example:     `"c7dd251f-811f-4ba2-a905-acd4d3f8f08b"`,
						Required:    true,
					},
					Metadata{
						Name:        "azureClientSecret",
						Description: "Client secret (application password)",
						Example:     `"Ecy3XG7zVZK3/vl/a2NSB+a1zXLa8RnMum/IgD0E"`,
						Required:    true,
						Sensitive:   true,
					},
				),
			},
			{
				Title:       "Azure AD: Client certificate",
				Description: `Authenticate using Azure AD with a client certificate. One of "azureCertificate" and "azureCertificateFile" is required.`,
				Metadata: mergedMetadata(bi.Metadata,
					Metadata{
						Name:        "azureTenantId",
						Description: "ID of the Azure AD tenant",
						Example:     `"cd4b2887-304c-47e1-b4d5-65447fdd542a"`,
						Required:    true,
					},
					Metadata{
						Name:        "azureClientId",
						Description: "Client ID (application ID)",
						Example:     `"c7dd251f-811f-4ba2-a905-acd4d3f8f08b"`,
						Required:    true,
					},
					Metadata{
						Name:        "azureCertificate",
						Description: "Certificate and private key (in either a PEM file containing both the certificate and key, or in PFX/PKCS#12 format)",
						Example:     `"-----BEGIN PRIVATE KEY-----\n MIIEvgI... \n -----END PRIVATE KEY----- \n -----BEGIN CERTIFICATE----- \n MIICoTC... \n -----END CERTIFICATE-----"`,
						Required:    false,
						Sensitive:   true,
					},
					Metadata{
						Name:        "azureCertificateFile",
						Description: "Path to PEM or PFX/PKCS#12 file on disk, containing the certificate and private key.",
						Example:     `"/path/to/file.pem"`,
						Required:    false,
						Sensitive:   false,
					},
					Metadata{
						Name:        "azureCertificatePassword",
						Description: "Password for the certificate if encrypted.",
						Example:     `"password"`,
						Required:    false,
						Sensitive:   true,
					},
				),
			},
		}
		return profiles, nil
	default:
		return nil, fmt.Errorf("built-in authentication profile %s does not exist", bi.Name)
	}
}

func mergedMetadata(base []Metadata, add ...Metadata) []Metadata {
	if len(base) == 0 {
		return add
	}

	res := make([]Metadata, 0, len(base)+len(add))
	res = append(res, base...)
	res = append(res, add...)
	return res
}
