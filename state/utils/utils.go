// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package utils

func Marshal(val interface{}, marshaler func(interface{}) ([]byte, error)) ([]byte, error) {
	var err error = nil
	bt, ok := val.([]byte)
	if !ok {
		bt, err = marshaler(val)
	}

	return bt, err
}
