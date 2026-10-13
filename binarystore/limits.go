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

package binarystore

import (
	"fmt"
	"io"
)

// NewSizeLimitReader returns a reader that passes through at most max bytes
// from r. Reading beyond max bytes fails with an error wrapping
// ErrFileTooLarge, so streaming uploads abort as soon as the limit is
// exceeded instead of after the whole body has been sent.
func NewSizeLimitReader(r io.Reader, max int64) io.Reader {
	return &sizeLimitReader{r: r, rem: max, max: max}
}

type sizeLimitReader struct {
	r   io.Reader
	rem int64
	max int64
	err error
}

func (r *sizeLimitReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.rem <= 0 {
		// The allowance is exhausted; probe the source to tell a body of
		// exactly max bytes (EOF) apart from one that exceeds the limit.
		var b [1]byte
		n, err := r.r.Read(b[:])
		if n > 0 {
			r.err = fmt.Errorf("%w: maximum size is %d bytes", ErrFileTooLarge, r.max)
			return 0, r.err
		}
		return 0, err
	}
	if int64(len(p)) > r.rem {
		p = p[:r.rem]
	}
	n, err := r.r.Read(p)
	r.rem -= int64(n)
	return n, err
}
