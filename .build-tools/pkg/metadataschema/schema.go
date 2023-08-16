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

// Package metadataschema contains the data structures for the schema of metadata.yaml / metadata.json files.
// Schemas are built using github.com/invopop/jsonschema .
package metadataschema

// ComponentMetadata is the schema for the metadata.yaml / metadata.json files.
type ComponentMetadata struct {
	// Version of the component metadata schema.
	SchemaVersion string `json:"schemaVersion" yaml:"schemaVersion" jsonschema:"enum=v1"`
	// Component type, of one of the allowed values.
	Type string `json:"type" yaml:"type" jsonschema:"enum=bindings,enum=state,enum=secretstores,enum=pubsub,enum=workflows,enum=configuration,enum=lock,enum=middleware,enum=crypto"`
	// Name of the component (without the inital type, e.g. "http" instead of "bindings.http").
	Name string `json:"name" yaml:"name"`
	// Version of the component, with the leading "v", e.g. "v1".
	Version string `json:"version" yaml:"version"`
	// Component status.
	Status string `json:"status" yaml:"status" jsonschema:"enum=stable,enum=beta,enum=alpha,enum=development-only"`
	// Title of the component, e.g. "HTTP".
	Title string `json:"title" yaml:"title"`
	// Additional description for the component, optional.
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	// URLs with additional resources for the component, such as docs.
	URLs []URL `json:"urls" yaml:"urls"`
	// Properties for bindings only.
	// This should not present unless "type" is "bindings".
	Binding *Binding `json:"binding,omitempty" yaml:"binding,omitempty"`
	// Component capabilities.
	// For state stores, the presence of "actorStateStore" implies that the metadata property "actorStateStore" can be set. In that case, do not manually specify "actorStateStore" as metadata option.
	Capabilities []string `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
	// Authentication profiles for the component.
	AuthenticationProfiles []AuthenticationProfile `json:"authenticationProfiles,omitempty" yaml:"authenticationProfiles,omitempty"`
	// Built-in authentication profiles to import.
	BuiltInAuthenticationProfiles []BuiltinAuthenticationProfile `json:"builtinAuthenticationProfiles,omitempty" yaml:"builtinAuthenticationProfiles,omitempty"`
	// Metadata options for the component.
	Metadata []Metadata `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// URL represents one URL with additional resources.
type URL struct {
	// Title of the URL.
	Title string `json:"title" yaml:"title"`
	// URL.
	URL string `json:"url" yaml:"url"`
}

// Binding represents properties that are specific to bindings
type Binding struct {
	// If "true", the binding can be used as input binding.
	Input bool `json:"input,omitempty" yaml:"input,omitempty"`
	// If "true", the binding can be used as output binding.
	Output bool `json:"output,omitempty" yaml:"output,omitempty"`
	// List of operations that the output binding support.
	// Required in output bindings, and not allowed in input-only bindings.
	Operations []BindingOperation `json:"operations" yaml:"operations"`
}

// BindingOperation represents an operation offered by an output binding.
type BindingOperation struct {
	// Name of the operation, such as "create", "post", "delete", etc.
	Name string `json:"name" yaml:"name"`
	// Descrption of the operation.
	Description string `json:"description" yaml:"description"`
}

// Metadata property.
type Metadata struct {
	// Name of the metadata property.
	Name string `json:"name" yaml:"name"`
	// Description of the property.
	Description string `json:"description" yaml:"description"`
	// If "true", the property is required
	Required bool `json:"required,omitempty" yaml:"required,omitempty"`
	// If "true", the property represents a sensitive value such as a password.
	Sensitive bool `json:"sensitive,omitempty" yaml:"sensitive,omitempty"`
	// Type of the property.
	// If this is empty, it's interpreted as "string".
	Type string `json:"type,omitempty" yaml:"type,omitempty" jsonschema:"enum=string,enum=number,enum=bool,enum=duration,enum=bytesize"`
	// Default value for the property.
	// If it's a string, don't forget to add quotes.
	Default string `json:"default,omitempty" yaml:"default,omitempty"`
	// Example value.
	Example string `json:"example" yaml:"example"`
	// If set, forces the value to be one of those specified in this allowlist.
	AllowedValues []string `json:"allowedValues,omitempty" yaml:"allowedValues,omitempty"`
	// If set, specifies that the property is only applicable to bindings of the type specified below.
	// At least one of "input" and "output" must be "true".
	Binding *MetadataBinding `json:"binding,omitempty" yaml:"binding,omitempty"`
	// URL with additional information, such as docs.
	URL *URL `json:"url,omitempty" yaml:"url,omitempty"`
	// If set, specifies that the property is deprecated and should not be used in new configurations.
	Deprecated bool `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
}

// MetadataBinding is the type for the "binding" property in the "metadata" object.
type MetadataBinding struct {
	// If "true", the property can be used with the binding as input binding only.
	Input bool `json:"input,omitempty" yaml:"input,omitempty"`
	// If "true", the property can be used with the binding as output binding only.
	Output bool `json:"output,omitempty" yaml:"output,omitempty"`
}

// AuthenticationProfile is the type for an authentication profile.
type AuthenticationProfile struct {
	// Title of the authentication profile.
	Title string `json:"title" yaml:"title"`
	// Additional description for the authentication profile, optional.
	Description string `json:"description" yaml:"description"`
	// Metadata options applicable when using this authentication profile.
	Metadata []Metadata `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// BuiltinAuthenticationProfile is a reference to a built-in authentication profile.
type BuiltinAuthenticationProfile struct {
	// Name of the built-in authentication profile.
	Name string `json:"name" yaml:"name"`
	// Additional metadata options applicable when using this authentication profile.
	Metadata []Metadata `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}
