package configuration

// GetResponse is the request object for getting configuration
type GetResponse struct {
	Items []*Item `json:"items"`
}
