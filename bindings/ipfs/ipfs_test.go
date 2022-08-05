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

package ipfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/utils"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if utils.IsTruthy(os.Getenv("IPFS_TEST")) {
		log.Println("IPFS_TEST env var is not set to a truthy value; skipping tests")
		os.Exit(0)
	}

	os.Exit(m.Run())
}

func TestSingleNodeGlobalNetwork(t *testing.T) {
	var b *IPFSBinding
	repoPath := t.TempDir()

	// CIDS contained in the QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG folder
	folderCids := []string{
		"QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V",
		"QmYCvbfNbCwFR45HiNP45rwJgvatpiW38D961L5qAhUM5Y",
		"QmY5heUM5qgRubMDD1og9fhCPA6QdkMp3QCwd4s7gJsyE7",
		"QmdncfsVm2h5Kqq9hPmU7oAVX2zTSVP3L869tgTbPYnsha",
		"QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB",
		"QmTumTjvcYCAvRRwQ8sDRxh8ezmrcr88YFU7iYNroGGTBZ",
	}
	sort.Strings(folderCids)

	t.Run("init node", func(t *testing.T) {
		b = NewIPFSBinding(logger.NewLogger("tests"))
		err := b.Init(bindings.Metadata{
			Properties: map[string]string{
				"repoPath": repoPath,
				"routing":  "dhtclient",
			},
		})
		require.NoError(t, err)
	})

	t.Run("get operation", func(t *testing.T) {
		t.Run("empty path", func(t *testing.T) {
			_, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "get",
			})
			if assert.Error(t, err) {
				assert.Equal(t, err.Error(), "metadata property 'path' is empty")
			}
		})

		t.Run("retrieve document by CID", func(t *testing.T) {
			data := getDocument(t, b, "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB")
			compareHash(t, data, "a48161fca5edd15f4649bb928c10769216fccdf317265fc75d747c1e6892f53c")
		})

		t.Run("retrieve document by IPLD", func(t *testing.T) {
			// Same document, but different addressing method
			data := getDocument(t, b, "/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG/readme")
			compareHash(t, data, "a48161fca5edd15f4649bb928c10769216fccdf317265fc75d747c1e6892f53c")
		})

		t.Run("cannot retrieve folder", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			_, err := b.Invoke(ctx, &bindings.InvokeRequest{
				Operation: "get",
				Metadata: map[string]string{
					"path": "/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
				},
			})

			if assert.Error(t, err) {
				assert.Equal(t, err.Error(), "path does not represent a file")
			}
		})

		// Retrieve files also to speed up pinning later
		t.Run("retrieve files", func(t *testing.T) {
			for _, e := range folderCids {
				getDocument(t, b, e)
			}
			getDocument(t, b, "QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v")
		})
	})

	t.Run("ls operation", func(t *testing.T) {
		t.Run("empty path", func(t *testing.T) {
			_, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "ls",
			})
			if assert.Error(t, err) {
				assert.Equal(t, err.Error(), "metadata property 'path' is empty")
			}
		})

		testLsOperationResponse := func(t *testing.T, list lsOperationResponse) {
			cids := make([]string, len(list))
			for i, e := range list {
				cids[i] = e.Cid
			}
			sort.Strings(cids)
			assert.True(t, reflect.DeepEqual(cids, folderCids), "received='%v' expected='%v'", cids, folderCids)
		}

		t.Run("list by CID", func(t *testing.T) {
			list := listPath(t, b, "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")
			require.NotEmpty(t, list)
			testLsOperationResponse(t, list)
		})

		t.Run("list by IPLD", func(t *testing.T) {
			list := listPath(t, b, "/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")
			require.NotEmpty(t, list)
			testLsOperationResponse(t, list)
		})
	})

	t.Run("pin operations", func(t *testing.T) {
		t.Run("pin-ls: nothing is pinned on a new node", func(t *testing.T) {
			list := listPins(t, b)
			assert.Empty(t, list)
		})

		t.Run("pin-add: pin file by CID", func(t *testing.T) {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "pin-add",
				Metadata: map[string]string{
					"path": "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB",
				},
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			require.Empty(t, res.Data)
		})

		t.Run("pin-ls: list added pin", func(t *testing.T) {
			list := listPins(t, b)
			assert.Len(t, list, 1)
			assert.Equal(t, "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB", list[0].Cid)
		})

		t.Run("pin-add: pin file by IPLD", func(t *testing.T) {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "pin-add",
				Metadata: map[string]string{
					"path": "/ipfs/QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v",
				},
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			require.Empty(t, res.Data)
		})

		t.Run("pin-add: recursively pin folder", func(t *testing.T) {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "pin-add",
				Metadata: map[string]string{
					"path": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
				},
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			require.Empty(t, res.Data)
		})

		// Add the folder to the list of expected items
		expect := make([]string, len(folderCids)+2)
		expect[0] = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
		expect[1] = "QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v"
		copy(expect[2:], folderCids)
		sort.Strings(expect)

		t.Run("pin-ls: list added folder", func(t *testing.T) {
			cids := listPinnedCids(t, b)
			assert.True(t, reflect.DeepEqual(cids, expect), "received='%v' expected='%v'", cids, expect)
		})

		t.Run("pin-rm: remove file", func(t *testing.T) {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "pin-rm",
				Metadata: map[string]string{
					"path": "QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v",
				},
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			require.Empty(t, res.Data)
		})

		// Remove the un-pinned file
		i := 0
		for _, e := range expect {
			if e != "QmdytmR4wULMd3SLo6ePF4s3WcRHWcpnJZ7bHhoj3QB13v" {
				expect[i] = e
				i++
			}
		}
		expect = expect[:i]

		t.Run("pin-ls: updated after removed file", func(t *testing.T) {
			cids := listPinnedCids(t, b)
			assert.True(t, reflect.DeepEqual(cids, expect), "received='%v' expected='%v'", cids, expect)
		})

		t.Run("pin-rm: recursively remove folder", func(t *testing.T) {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "pin-rm",
				Metadata: map[string]string{
					"path": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
				},
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			require.Empty(t, res.Data)
		})

		t.Run("pin-rm: remove explicitly-pinned file", func(t *testing.T) {
			res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
				Operation: "pin-rm",
				Metadata: map[string]string{
					"path": "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB",
				},
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			require.Empty(t, res.Data)
		})

		t.Run("pin-ls: updated list is empty", func(t *testing.T) {
			list := listPins(t, b)
			assert.Empty(t, list)
		})
	})

	t.Run("create operation", func(t *testing.T) {
		expectPins := []string{}

		t.Run("add with default options", func(t *testing.T) {
			path := addDocument(t, b,
				[]byte("Quel ramo del lago di Como, che volge a mezzogiorno"),
				nil,
			)

			assert.Equal(t, "/ipfs/QmRW7jvkePyaAFvtapaqZ9kNkziUrmkhi4ue5oNpXS2qUx", path)

			expectPins = append(expectPins, "QmRW7jvkePyaAFvtapaqZ9kNkziUrmkhi4ue5oNpXS2qUx")
		})

		t.Run("add with CID v1", func(t *testing.T) {
			path := addDocument(t, b,
				[]byte("Quel ramo del lago di Como, che volge a mezzogiorno"),
				map[string]string{"cidVersion": "1"},
			)

			assert.Equal(t, "/ipfs/bafkreidhuwuwgycmsbj4sesi3pm6vpxpbm6byt3twex7sc2nadaxksnqeq", path)

			expectPins = append(expectPins, "bafkreidhuwuwgycmsbj4sesi3pm6vpxpbm6byt3twex7sc2nadaxksnqeq")
		})

		t.Run("added files are pinned", func(t *testing.T) {
			cids := listPinnedCids(t, b)
			assert.True(t, reflect.DeepEqual(cids, expectPins), "received='%v' expected='%v'", cids, expectPins)
		})

		t.Run("add without pinning", func(t *testing.T) {
			path := addDocument(t, b,
				[]byte("😁🐶"),
				map[string]string{"pin": "false"},
			)

			assert.Equal(t, "/ipfs/QmWsLpV1UUD26qHaEJqXfHazSRRZVX82M51EQ87UT7ryiR", path)
		})

		t.Run("pinned documents haven't changed", func(t *testing.T) {
			cids := listPinnedCids(t, b)
			assert.True(t, reflect.DeepEqual(cids, expectPins), "received='%v' expected='%v'", cids, expectPins)
		})

		t.Run("add inline", func(t *testing.T) {
			path := addDocument(t, b,
				[]byte("😁🐶"),
				map[string]string{"inline": "true"},
			)

			assert.Equal(t, "/ipfs/bafyaaeakbyeaeeqi6cpzrapqt6ilmgai", path)
		})
	})

	if b != nil {
		b.Close()
	}
}

func getDocument(t *testing.T, b *IPFSBinding, path string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := b.Invoke(ctx, &bindings.InvokeRequest{
		Operation: "get",
		Metadata: map[string]string{
			"path": path,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Data)

	return res.Data
}

func listPath(t *testing.T, b *IPFSBinding, path string) lsOperationResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := b.Invoke(ctx, &bindings.InvokeRequest{
		Operation: "ls",
		Metadata: map[string]string{
			"path": path,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Data)

	list := lsOperationResponse{}
	err = json.Unmarshal(res.Data, &list)
	require.NoError(t, err)

	return list
}

func listPins(t *testing.T, b *IPFSBinding) pinLsOperationResponse {
	res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
		Operation: "pin-ls",
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Data)

	list := pinLsOperationResponse{}
	err = json.Unmarshal(res.Data, &list)
	require.NoError(t, err)

	return list
}

func listPinnedCids(t *testing.T, b *IPFSBinding) []string {
	list := listPins(t, b)
	require.NotEmpty(t, list)

	cids := make([]string, len(list))
	for i, e := range list {
		cids[i] = e.Cid
	}

	sort.Strings(cids)
	return cids
}

func addDocument(t *testing.T, b *IPFSBinding, data []byte, metadata map[string]string) string {
	res, err := b.Invoke(context.Background(), &bindings.InvokeRequest{
		Operation: "create",
		Data:      data,
		Metadata:  metadata,
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Data)

	o := addOperationResponse{}
	err = json.Unmarshal(res.Data, &o)
	require.NoError(t, err)
	require.NotEmpty(t, o.Path)

	return o.Path
}

func compareHash(t *testing.T, data []byte, expect string) {
	require.NotEmpty(t, data)
	h := sha256.Sum256(data)
	digest := hex.EncodeToString(h[:])
	assert.Equal(t, expect, digest)
}
