// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package secretstores

import (
	"encoding/json"

	"github.com/awnumar/memguard"
)

type SecretKey struct{ *memguard.Enclave }

func NewSecretKey(key []byte) SecretKey {
	return SecretKey{memguard.NewEnclave(key)}
}

func (e *SecretKey) MarshalJSON() ([]byte, error) {
	buf, err := e.Open()
	if err != nil {
		return nil, err
	}
	defer buf.Destroy()
	return json.Marshal(buf.String())
}

func (e *SecretKey) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	*e = SecretKey{memguard.NewEnclave([]byte(s))}
	return nil
}
