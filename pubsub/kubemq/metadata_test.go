package kubemq

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

func Test_createMetadata(t *testing.T) {
	tests := []struct {
		name    string
		meta    pubsub.Metadata
		want    *metadata
		wantErr bool
	}{
		{
			name: "create valid metadata",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":           "localhost:50000",
						"channel":           "test",
						"clientID":          "clientID",
						"authToken":         "authToken",
						"group":             "group",
						"store":             "true",
						"useMock":           "true",
						"disableReDelivery": "true",
					},
				},
			},
			want: &metadata{
				host:              "localhost",
				port:              50000,
				clientID:          "clientID",
				authToken:         "authToken",
				group:             "group",
				isStore:           true,
				disableReDelivery: true,
			},
			wantErr: false,
		},
		{
			name: "create valid metadata with empty group",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":   "localhost:50000",
						"clientID":  "clientID",
						"authToken": "authToken",
						"store":     "false",
					},
				},
			},
			want: &metadata{
				host:      "localhost",
				port:      50000,
				clientID:  "clientID",
				authToken: "authToken",
				group:     "",
				isStore:   false,
			},
			wantErr: false,
		},
		{
			name: "create valid metadata with empty authToken",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":  "localhost:50000",
						"channel":  "test",
						"clientID": "clientID",
						"group":    "group",
						"store":    "true",
					},
				},
			},
			want: &metadata{
				host:      "localhost",
				port:      50000,
				clientID:  "clientID",
				authToken: "",
				group:     "group",
				isStore:   true,
			},
			wantErr: false,
		},
		{
			name: "create invalid metadata with bad host",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":  ":50000",
						"clientID": "clientID",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid metadata with bad port",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":  "localhost:badport",
						"clientID": "clientID",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid metadata with empty address",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":  "",
						"clientID": "clientID",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid metadata with bad address format",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":  "localhost:50000:badport",
						"clientID": "clientID",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid metadata with bad store info",
			meta: pubsub.Metadata{
				Base: mdata.Base{
					Properties: map[string]string{
						"address":  "localhost:50000",
						"clientID": "clientID",
						"store":    "bad",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createMetadata(tt.meta)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
