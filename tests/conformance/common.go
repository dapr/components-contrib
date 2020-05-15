package conformance

import (
	"math/rand"

	"github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	"github.com/dapr/dapr/pkg/components"
	config "github.com/dapr/dapr/pkg/config/modes"
)

func LoadComponents(componentPath string) ([]v1alpha1.Component, error) {
	cfg := config.StandaloneConfig{
		ComponentsPath: componentPath,
	}
	standaloneComps := components.NewStandaloneComponents(cfg)
	components, err := standaloneComps.LoadComponents()
	if err != nil {
		return nil, err
	}
	return components, nil
}

func ConvertMetadataToProperties(items []v1alpha1.MetadataItem) map[string]string {
	properties := map[string]string{}
	for _, c := range items {
		properties[c.Name] = c.Value
	}
	return properties
}

func NewRandString(length int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}