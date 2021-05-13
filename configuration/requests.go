package configuration

// ConfigurationItem represents a configuration item with name, content and other information.
type Item struct {
	Name     string            `json:"name"`
	Content  string            `json:"content,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
	Metadata map[string]string `json:"metadata"`
}

// GetRequest is the object describing a get configuration request
type GetRequest struct {
	AppID    string            `json:"appID"`
	Metadata map[string]string `json:"metadata"`
}

// SubscribeRequest is the object describing a subscribe configuration request
type SubscribeRequest struct {
	AppID    string            `json:"appID"`
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// UpdateEvent is the object describing a configuration update event
type UpdateEvent struct {
	AppID    string  `json:"appID"`
	Revision string  `json:"revision"`
	Items    []*Item `json:"items"`
}

// flowing code is for admin api and test

// SaveRequest is the object describing a save configuration request
type SaveRequest struct {
	AppID    string            `json:"appID"`
	Items    []*Item           `json:"items"`
	Metadata map[string]string `json:"metadata"`
}

// DeleteRequest is the object describing a delete configuration request
// It will remove all the configuration of specified application
// If you want to delete some configuration items, please use save() method
type DeleteRequest struct {
	AppID    string            `json:"appID"`
	Metadata map[string]string `json:"metadata"`
}
