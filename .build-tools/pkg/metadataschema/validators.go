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

package metadataschema

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	mdutils "github.com/dapr/components-contrib/metadata"
)

const (
	bindingDirectionMetadataKey = "direction"
	bindingDirectionInput       = "input"
	bindingDirectionOutput      = "output"
	bindingRouteMetadataKey     = "route"
)

// IsValid performs additional validation and returns true if the object is valid.
func (c *ComponentMetadata) IsValid() error {
	// Check valid  component type
	compType := mdutils.ComponentType(c.Type)
	if c.Type == "" || !compType.IsValid() {
		return errors.New("type is empty")
	}

	// Sanity check for bindings
	if compType == mdutils.BindingType && c.Binding == nil {
		return errors.New("property 'binding' is required for components of type 'bindings'")
	}
	if compType != mdutils.BindingType && c.Binding != nil {
		return errors.New("property 'binding' is not allowed in components that are not of type 'bindings'")
	}

	// Ensure that there's a URL called Reference to the Dapr docs
	if len(c.URLs) < 1 {
		return errors.New("property 'urls' must contain at least one URL to the Dapr docs with title 'Reference'")
	}
	hasReferenceUrl := false
	for _, u := range c.URLs {
		if u.Title == "Reference" && strings.HasPrefix(u.URL, "https://docs.dapr.io/reference/components-reference/") {
			hasReferenceUrl = true
		}
	}
	if !hasReferenceUrl {
		return errors.New("property 'urls' must a link to the Dapr docs with title 'Reference' and URL starting with 'https://docs.dapr.io/reference/components-reference/'")
	}

	// Append built-in metadata properties
	err := c.AppendBuiltin()
	if err != nil {
		return err
	}

	// Append built-in authentication profiles
	for _, profile := range c.BuiltInAuthenticationProfiles {
		appendProfiles, err := ParseBuiltinAuthenticationProfile(profile, c.Title)
		if err != nil {
			return err
		}
		c.AuthenticationProfiles = append(c.AuthenticationProfiles, appendProfiles...)
	}
	// Remove the property builtinAuthenticationProfiles now
	c.BuiltInAuthenticationProfiles = nil

	// Trim newlines from all descriptions
	c.Description = strings.TrimSpace(c.Description)
	for i := range c.AuthenticationProfiles {
		c.AuthenticationProfiles[i].Description = strings.TrimSpace(c.AuthenticationProfiles[i].Description)
		for j := range c.AuthenticationProfiles[i].Metadata {
			c.AuthenticationProfiles[i].Metadata[j].Description = strings.TrimSpace(c.AuthenticationProfiles[i].Metadata[j].Description)
		}
	}
	for i := range c.Metadata {
		c.Metadata[i].Description = strings.TrimSpace(c.Metadata[i].Description)
	}

	return nil
}

// AppendBuiltin appends built-in metadata properties for the given type.
func (c *ComponentMetadata) AppendBuiltin() error {
	compType := mdutils.ComponentType(c.Type)
	switch compType {
	case mdutils.StateStoreType:
		if c.Metadata == nil {
			c.Metadata = []Metadata{}
		}

		if slices.Contains(c.Capabilities, "actorStateStore") {
			c.Metadata = append(c.Metadata,
				Metadata{
					Name:        "actorStateStore",
					Type:        "bool",
					Description: "Use this state store for actors. Defaults to `false`.",
					Example:     `"false"`,
				},
			)
		}
		if slices.Contains(c.Capabilities, "transactional") {
			c.Metadata = append(c.Metadata,
				Metadata{
					Name:        "outboxPublishPubsub",
					Type:        "string",
					Description: "For outbox. Sets the name of the pub/sub component to deliver the notifications when publishing state changes",
				},
				Metadata{
					Name:        "outboxPublishTopic",
					Type:        "string",
					Description: `For outbox. Sets the topic that receives the state changes on the pub/sub configured with "outboxPublishPubsub". The message body will be a state transaction item for an insert or update operation`,
				},
				Metadata{
					Name:        "outboxPubsub",
					Type:        "string",
					Description: `For outbox. Sets the pub/sub component used by Dapr to coordinate the state and pub/sub transactions. If not set, the pub/sub component configured with "outboxPublishPubsub" is used. This is useful if you want to separate the pub/sub component used to send the notification state changes from the one used to coordinate the transaction`,
					Default:     "outboxPublishPubsub",
				},
				Metadata{
					Name:        "outboxDiscardWhenMissingState",
					Description: "By setting outboxDiscardWhenMissingState to true, Dapr discards the transaction if it cannot find the state in the database and does not retry. This setting can be useful if the state store data has been deleted for any reason before Dapr was able to deliver the message and you would like Dapr to drop the items from the pub/sub and stop retrying to fetch the state",
					Type:        "bool",
					Default:     "false",
				},
			)
		}

		c.Metadata = append(c.Metadata,
			Metadata{
				Name:        "keyPrefix",
				Type:        "string",
				Description: "Prefix added to keys in the state store.",
				Example:     `"appid"`,
				Default:     "appid",
				URL: &URL{
					Title: "Documentation",
					URL:   "https://docs.dapr.io/developing-applications/building-blocks/state-management/howto-share-state/",
				},
			},
		)
	case mdutils.LockStoreType:
		if c.Metadata == nil {
			c.Metadata = []Metadata{}
		}
		c.Metadata = append(c.Metadata,
			Metadata{
				Name:        "keyPrefix",
				Type:        "string",
				Description: "Prefix added to keys in the state store.",
				Example:     `"appid"`,
				Default:     "appid",
			},
		)
	case mdutils.PubSubType:
		if c.Metadata == nil {
			c.Metadata = []Metadata{}
		}
		c.Metadata = append(c.Metadata,
			Metadata{
				Name:        "consumerID",
				Type:        "string",
				Description: "Set the consumer ID to control namespacing. Defaults to the app's ID.",
				Example:     `"{namespace}"`,
				URL: &URL{
					Title: "Documentation",
					URL:   "https://docs.dapr.io/developing-applications/building-blocks/pubsub/howto-namespace/",
				},
			},
			Metadata{
				Name:        "allowedTopics",
				Type:        "string",
				Description: "A comma-separated list of allowed topics for all applications. If empty (default) apps can publish and subscribe to all topics, notwithstanding `publishingScopes` and `subscriptionScopes`.",
				Example:     `"app1=topic1;app2=topic2,topic3"`,
				URL: &URL{
					Title: "Documentation",
					URL:   "https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-scopes/",
				},
			},
			Metadata{
				Name:        "publishingScopes",
				Type:        "string",
				Description: "A semicolon-separated list of applications and comma-separated topic lists, allowing that app to publish to that list of topics. If empty (default), apps can publish to all topics.",
				Example:     `"app1=topic1;app2=topic2,topic3;app3="`,
				URL: &URL{
					Title: "Documentation",
					URL:   "https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-scopes/",
				},
			},
			Metadata{
				Name:        "subscriptionScopes",
				Type:        "string",
				Description: "A semicolon-separated list of applications and comma-separated topic lists, allowing that app to subscribe to that list of topics. If empty (default), apps can subscribe to all topics.",
				Example:     `"app1=topic1;app2=topic2,topic3"`,
				URL: &URL{
					Title: "Documentation",
					URL:   "https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-scopes/",
				},
			},
			Metadata{
				Name:        "protectedTopics",
				Type:        "string",
				Description: `A comma-separated list of topics marked as "protected" for all applications. If a topic is marked as protected then an application must be explicitly granted publish or subscribe permissions through 'publishingScopes' or 'subscriptionScopes' to publish or subscribe to it.`,
				Example:     `"topic1,topic2"`,
				URL: &URL{
					Title: "Documentation",
					URL:   "https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-scopes/",
				},
			},
		)
	case mdutils.BindingType:
		if c.Binding != nil {
			if c.Metadata == nil {
				c.Metadata = []Metadata{}
			}

			if c.Binding.Input {
				direction := bindingDirectionInput
				allowedValues := []string{
					bindingDirectionInput,
				}

				if c.Binding.Output {
					direction = fmt.Sprintf("%s,%s", bindingDirectionInput, bindingDirectionOutput)
					allowedValues = append(allowedValues, bindingDirectionOutput, direction)
				}

				c.Metadata = append(c.Metadata,
					Metadata{
						Name:        bindingDirectionMetadataKey,
						Type:        "string",
						Description: "Indicates the direction of the binding component.",
						Example:     `"` + direction + `"`,
						URL: &URL{
							Title: "Documentation",
							URL:   "https://docs.dapr.io/reference/api/bindings_api/#binding-direction-optional",
						},
						AllowedValues: allowedValues,
					},
				)

				c.Metadata = append(c.Metadata,
					Metadata{
						Name:        bindingRouteMetadataKey,
						Type:        "string",
						Description: "Specifies a custom route for incoming events.",
						Example:     `"/custom-path"`,
						URL: &URL{
							Title: "Documentation",
							URL:   "https://docs.dapr.io/developing-applications/building-blocks/bindings/howto-triggers/#specify-a-custom-route",
						},
					},
				)
			}
		}
	}

	// Sanity check to ensure the data is in sync
	builtin := compType.BuiltInMetadataProperties()
	allKeys := make(map[string]struct{}, len(c.Metadata))
	for _, v := range c.Metadata {
		allKeys[v.Name] = struct{}{}
	}
	for _, k := range builtin {
		_, ok := allKeys[k]
		if k == "actorStateStore" {
			hasCapability := slices.Contains(c.Capabilities, "actorStateStore")
			if hasCapability && !ok {
				return errors.New("expected to find built-in property 'actorStateStore'")
			} else if !hasCapability && ok {
				return errors.New("found property 'actorStateStore' in component that does not have the 'actorStateStore' capability")
			}
		} else if !ok {
			return fmt.Errorf("expected to find built-in property %s", k)
		}
	}

	return nil
}
