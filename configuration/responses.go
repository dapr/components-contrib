package configuration

// GetResponse is the request object for getting configuration
type GetResponse struct {
	AppID    string  `json:"appID"`
	Revision string  `json:"revision"`
	Items    []*Item `json:"items"`
}
