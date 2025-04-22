/*
Copyright 2025 The Dapr Authors
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

package oracledatabase

import (
	"net/url"
	"testing"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionString(t *testing.T) {
	tests := []struct {
		name           string
		metadata       map[string]string
		expectedConn   string
		expectedError  string
		withWallet     bool
		walletLocation string
	}{
		{
			name: "Simple URL format",
			metadata: map[string]string{
				"connectionString": "oracle://system:pass@localhost:1521/FREEPDB1",
			},
			expectedConn: "oracle://system:pass@localhost:1521/FREEPDB1?",
		},
		{
			name: "Pure descriptor format",
			metadata: map[string]string{
				"connectionString": "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
			},
			expectedConn: "oracle://:@:0/?connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dlocalhost%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "URL with descriptor format",
			metadata: map[string]string{
				"connectionString": "oracle://system:pass@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
			},
			expectedConn: "oracle://system:pass@:0/?connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dlocalhost%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "Complex descriptor with load balancing and failover",
			metadata: map[string]string{
				"connectionString": "(DESCRIPTION=(CONNECT_TIMEOUT=30)(RETRY_COUNT=20)(RETRY_DELAY=3)(FAILOVER=ON)(LOAD_BALANCE=OFF)(ADDRESS_LIST=(LOAD_BALANCE=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=db1.example.com)(PORT=1521)))(ADDRESS_LIST=(LOAD_BALANCE=ON)(ADDRESS=(PROTOCOL=TCP)(HOST=db2.example.com)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1_service)))",
			},
			expectedConn: "oracle://:@:0/?connStr=%28DESCRIPTION%3D%28CONNECT_TIMEOUT%3D30%29%28RETRY_COUNT%3D20%29%28RETRY_DELAY%3D3%29%28FAILOVER%3DON%29%28LOAD_BALANCE%3DOFF%29%28ADDRESS_LIST%3D%28LOAD_BALANCE%3DON%29%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Ddb1.example.com%29%28PORT%3D1521%29%29%29%28ADDRESS_LIST%3D%28LOAD_BALANCE%3DON%29%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Ddb2.example.com%29%28PORT%3D1521%29%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1_service%29%29%29",
		},
		{
			name: "Simple URL with wallet",
			metadata: map[string]string{
				"connectionString":     "oracle://system:pass@localhost:1521/service",
				"oracleWalletLocation": "/path/to/wallet",
			},
			withWallet:     true,
			walletLocation: "/path/to/wallet",
			expectedConn:   "oracle://system:pass@localhost:1521/service?WALLET=%2Fpath%2Fto%2Fwallet&TRACE FILE=trace.log&SSL=enable&SSL Verify=false",
		},
		{
			name: "Descriptor with wallet",
			metadata: map[string]string{
				"connectionString":     "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=test.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))",
				"oracleWalletLocation": "/path/to/wallet",
			},
			withWallet:     true,
			walletLocation: "/path/to/wallet",
			expectedConn:   "oracle://:@:0/?WALLET=%2Fpath%2Fto%2Fwallet&TRACE FILE=trace.log&SSL=enable&SSL Verify=false&connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dtest.example.com%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "URL with descriptor and existing parameters",
			metadata: map[string]string{
				"connectionString":     "oracle://system:pass@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=test.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))?param1=value1",
				"oracleWalletLocation": "/path/to/wallet",
			},
			withWallet:     true,
			walletLocation: "/path/to/wallet",
			expectedConn:   "oracle://system:pass@:0/?param1=value1&TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=%2Fpath%2Fto%2Fwallet&connStr=%28DESCRIPTION%3D%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Dtest.example.com%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DFREEPDB1%29%29%29",
		},
		{
			name: "Compressed descriptor format",
			metadata: map[string]string{
				"connectionString": "(DESCRIPTION=(CONNECT_TIMEOUT=90)(RETRY_COUNT=20)(RETRY_DELAY=3)(TRANSPORT_CONNECT_TIMEOUT=3)(ADDRESS=(PROTOCOL=TCP)(HOST=db.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=MYSERVICE)))",
			},
			expectedConn: "oracle://@:0/?connStr=%28DESCRIPTION%3D%28CONNECT_TIMEOUT%3D90%29%28RETRY_COUNT%3D20%29%28RETRY_DELAY%3D3%29%28TRANSPORT_CONNECT_TIMEOUT%3D3%29%28ADDRESS%3D%28PROTOCOL%3DTCP%29%28HOST%3Ddb.example.com%29%28PORT%3D1521%29%29%28CONNECT_DATA%3D%28SERVICE_NAME%3DMYSERVICE%29%29%29",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create metadata
			metadata := state.Metadata{
				Base: metadata.Base{
					Properties: tt.metadata,
				},
			}

			meta, err := parseMetadata(metadata.Properties)
			require.NoError(t, err)

			actualConnectionString, err := parseConnectionString(meta)
			require.NoError(t, err)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			} else {
				require.NoError(t, err)
			}

			expectedURL, err := url.Parse(tt.expectedConn)
			require.NoError(t, err)
			actualURL, err := url.Parse(actualConnectionString)
			require.NoError(t, err)

			assert.Equal(t, expectedURL.Scheme, actualURL.Scheme)
			assert.Equal(t, expectedURL.Host, actualURL.Host)
			assert.Equal(t, expectedURL.Path, actualURL.Path)
			assert.Equal(t, expectedURL.User.Username(), actualURL.User.Username())
			ep, _ := expectedURL.User.Password()
			ap, _ := actualURL.User.Password()
			assert.Equal(t, ep, ap)

			query, err := url.ParseQuery(expectedURL.RawQuery)
			require.NoError(t, err)

			for k, v := range query {
				assert.Equal(t, v, actualURL.Query()[k])
			}

			if tt.withWallet {
				assert.Equal(t, tt.walletLocation, meta.OracleWalletLocation)
				assert.Contains(t, actualConnectionString, "WALLET=")
				assert.Contains(t, actualConnectionString, "SSL=enable")
			}
		})
	}
}
