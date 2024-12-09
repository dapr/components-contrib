package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	`"Text"`:    Text,
	`"Numeric"`: Numeric,
	`"Bool"`:    Bool,
	`"Object"`:  Object,
	`"Array"`:   Array,
}

var typeToString = map[Type]string{
	Text:    `"Text"`,
	Numeric: `"Numeric"`,
	Bool:    `"Bool"`,
	Object:  `"Object"`,
	Array:   `"Array"`,
}

func parseType(typeString string) (Type, error) {
	if s, ok := stringToType[typeString]; ok {
		return s, nil
	}
	return Text, fmt.Errorf("invalid type, default text: %s", typeString)
}

func evalType(typeValue Type) (string, error) {
	if s, ok := typeToString[typeValue]; ok {
		return s, nil
	}
	return "nil", errors.New("invalid type, default text: nil")
}

func (t Type) MarshalJSON() ([]byte, error) {
	if s, error := evalType(t); error != nil {
		return nil, error
	} else {
		return []byte(s), nil
	}
}

func (t *Type) UnmarshalJSON(p []byte) error {
	elem := string(p)
	if elem == `null` || elem == `""` {
		return nil
	}
	var err error
	*t, err = parseType(strings.Trim(elem, strconv.Itoa(int('"'))))
	return err
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
