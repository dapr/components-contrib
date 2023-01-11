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

package crypto

/*!
This code is adapted from https://github.com/mergermarket/go-pkcs7/tree/153b18ea13c9b94f698070cadb23701e51a55b3e
Copyright (c) 2017 Richard Zadorozny
License: MIT https://github.com/mergermarket/go-pkcs7/blob/153b18ea13c9b94f698070cadb23701e51a55b3e/LICENSE
*/

import (
	"errors"
)

// PadPKCS7 adds PKCS#7 padding to a message.
func PadPKCS7(buf []byte, size int) ([]byte, error) {
	bufLen := len(buf)
	padLen := size - bufLen%size
	padded := make([]byte, bufLen+padLen)
	copy(padded, buf)
	for i := 0; i < padLen; i++ {
		padded[bufLen+i] = byte(padLen)
	}
	return padded, nil
}

// UnpadPKCS7 removes PKCS#7 from a message.
func UnpadPKCS7(buf []byte, size int) ([]byte, error) {
	if len(buf)%size != 0 {
		return nil, errors.New("pkcs7: incorrect padding value")
	}

	unpaddedLen := len(buf) - int(buf[len(buf)-1])
	return buf[:unpaddedLen], nil
}
