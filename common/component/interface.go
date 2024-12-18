package component

import "github.com/dapr/components-contrib/build-tools/pkg/metadataschema"

type MetadataBuilder interface {
	Defaults() any
	Examples() any
	Binding() metadataschema.Binding
}
