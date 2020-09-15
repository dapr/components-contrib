package utils

func Marshal(val interface{}, marshaler func(interface{}) ([]byte, error)) ([]byte, error) {
	var err error
	bt, ok := val.([]byte)
	err = nil
	if !ok {
		bt, err = marshaler(val)
	}

	return bt, err
}
