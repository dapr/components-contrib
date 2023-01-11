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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const BLOCK_SIZE = 16

func TestPkcs7(t *testing.T) {
	t.Run("Pads", func(t *testing.T) {
		result, _ := PadPKCS7([]byte("1234567890"), BLOCK_SIZE)
		expected := []byte("1234567890\x06\x06\x06\x06\x06\x06")
		assert.Equal(t, expected, result)
	})

	t.Run("Unpads", func(t *testing.T) {
		result, _ := UnpadPKCS7([]byte("1234567890\x06\x06\x06\x06\x06\x06"), BLOCK_SIZE)
		expected := []byte("1234567890")
		assert.Equal(t, expected, result)
	})

	t.Run("Handles long", func(t *testing.T) {
		longStr := []byte("123456789012345678901234567890123456789012345678901234567890")
		padded, _ := PadPKCS7(longStr, BLOCK_SIZE)
		expected := []byte("123456789012345678901234567890123456789012345678901234567890\x04\x04\x04\x04")
		assert.Equal(t, expected, padded)
		if bytes.Equal(padded, expected) == false {
			panic(fmt.Sprintf(`Padding wrong - expected "%x" but got "%x"`, expected, padded))
		}

		unpadded, _ := UnpadPKCS7(padded, BLOCK_SIZE)
		assert.Equal(t, longStr, unpadded)
	})

	t.Run("Handles short", func(t *testing.T) {
		shortStr := []byte("1")
		padded, _ := PadPKCS7(shortStr, BLOCK_SIZE)
		expected := []byte("1\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f\x0f")
		assert.Equal(t, expected, padded)

		unpadded, _ := UnpadPKCS7(padded, BLOCK_SIZE)
		assert.Equal(t, shortStr, unpadded)
	})

	t.Run("Handles empty", func(t *testing.T) {
		emptyStr := []byte("")
		padded, _ := PadPKCS7(emptyStr, BLOCK_SIZE)
		expected := []byte("\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10")
		assert.Equal(t, expected, padded)

		unpadded, _ := UnpadPKCS7(padded, BLOCK_SIZE)
		assert.Equal(t, emptyStr, unpadded)
	})

	t.Run("Handles block size", func(t *testing.T) {
		val := []byte("1234567890ABCDEF")
		padded, _ := PadPKCS7(val, BLOCK_SIZE)
		expected := []byte("1234567890ABCDEF\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10\x10")
		assert.Equal(t, expected, padded)

		unpadded, _ := UnpadPKCS7(padded, BLOCK_SIZE)
		assert.Equal(t, val, unpadded)
	})
}
