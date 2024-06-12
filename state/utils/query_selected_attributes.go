package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type Type int

const (
	Text Type = iota
	Numeric
	Bool
	Object
	Array
)

var stringToType = map[string]Type{
	"Text":    Text,
	"Numeric": Numeric,
	"Bool":    Bool,
	"Object":  Object,
	"Array":   Array,
}

func ParseType(typeString string) (Type, error) {
	if s, ok := stringToType[typeString]; ok {
		return s, nil
	}
	return Text, fmt.Errorf("invalid type, default text: %s", typeString)
}

type Attribute struct {
	Name string `json:"name,omitempty"`
	Path string `json:"path,omitempty"`
	Type Type   `json:"type,omitempty"`
}

func ParseQuerySelectedAttributes(content string) ([]Attribute, error) {
	if len(content) == 0 {
		return nil, nil
	}

	var attributes []Attribute
	dec := json.NewDecoder(bytes.NewReader([]byte(content)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&attributes); err != nil {
		return nil, fmt.Errorf("incorrect syntax selected attributes json  '%s': %w", content, err)
	}

	return attributes, nil
}
