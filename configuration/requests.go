package configuration

// ConfigurationItem represents a configuration item with key, content and other information.
type Item struct {
	Key      string            `json:"key"`
	Content  string            `json:"content,omitempty"`
	Group    string            `json:"group,omitempty"`
	Label    string            `json:"label,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
	Metadata map[string]string `json:"metadata"`
}

// SaveRequest is the object describing a save configuration request
type SaveRequest struct {
	AppID    string            `json:"appID"`
	Items    []*Item            `json:"items"`
	Metadata map[string]string `json:"metadata"`
}

// GetRequest is the object describing a get configuration request
type GetRequest struct {
	AppID           string            `json:"appID"`
	Group           string            `json:"group,omitempty"`
	Label           string            `json:"group,omitempty"`
	Keys            []string          `json:"keys"`
	Metadata        map[string]string `json:"metadata"`
	SubscribeUpdate bool              `json:"subscribe_update"`
}

// DeleteRequest is the object describing a delete configuration request
type DeleteRequest struct {
	AppID    string            `json:"appID"`
	Group    string            `json:"group,omitempty"`
	Label    string            `json:"group,omitempty"`
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// UpdateEvent is the object describing a configuration update event
type UpdateEvent struct {
	AppID string `json:"appID"`
	Items []*Item `json:"items"`
}
