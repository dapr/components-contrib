package utils

import (
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/metadata"
	"github.com/pkg/errors"
)

// IsTruthy returns true if a string is a truthy value.
// Truthy values are "y", "yes", "true", "t", "on", "1" (case-insensitive); everything else is false.
func IsTruthy(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "y", "yes", "true", "t", "on", "1":
		return true
	default:
		return false
	}
}

// Validate validates metadata, and if it checks pass, it will nil.
func Validate(meta map[string]string,
	validationMap map[string]metadata.ValidationType) error {
	var err error
	for key, typ := range validationMap {
		if value, exist := meta[key]; !exist {
			if err = handleMissKey(value, typ); err != nil {
				return err
			}
		} else {
			if err = handleExistKey(value, typ); err != nil {
				return err
			}
		}
	}

	return nil
}

func handleMissKey(input string, typ metadata.ValidationType) error {
	switch typ {
	case metadata.Required:
		return errors.Errorf("[handleMissKey] metadata key: %s required", input)
	case metadata.Optional:
		return nil
	case metadata.Denied:
		return nil
	default:
		return fmt.Errorf("[handleMissKey] validation type invalid: %s", typ)
	}
}

func handleExistKey(input string, typ metadata.ValidationType) error {
	switch typ {
	case metadata.Required:
		return nil
	case metadata.Optional:
		return nil
	case metadata.Denied:
		return fmt.Errorf("[handleExistKey] metadata key: %s denied", input)
	default:
		return fmt.Errorf("[handleExistKey] validation type invalid: %s", typ)
	}
}
