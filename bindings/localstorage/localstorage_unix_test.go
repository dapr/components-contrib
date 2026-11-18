//go:build linux || darwin

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

package localstorage

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
)

func TestFIFOIsRejected(t *testing.T) {
	ls := newTestLocalStorage(t)
	require.NoError(t, syscall.Mkfifo(filepath.Join(ls.metadata.RootPath, "fifo"), 0o600))

	// Neither operation may wait for the other end of the FIFO.
	for _, op := range []bindings.OperationKind{bindings.GetOperation, bindings.CreateOperation} {
		done := make(chan error, 1)
		go func() {
			_, err := invoke(t, ls, op, "fifo", []byte("data"))
			done <- err
		}()
		select {
		case err := <-done:
			require.Error(t, err, op)
		case <-time.After(10 * time.Second):
			t.Fatalf("%s on a FIFO did not return", op)
		}
	}
}
