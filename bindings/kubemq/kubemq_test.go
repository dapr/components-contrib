package kubemq

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
)

func Test_createOptions(t *testing.T) {
	tests := []struct {
		name    string
		meta    bindings.Metadata
		want    *options
		wantErr bool
	}{
		{
			name: "create valid opts",
			meta: bindings.Metadata{
				Base: metadata.Base{
					Name: "kubemq",
					Properties: map[string]string{
						"address":            "localhost:50000",
						"channel":            "test",
						"authToken":          "authToken",
						"pollMaxItems":       "10",
						"autoAcknowledged":   "true",
						"pollTimeoutSeconds": "10",
					},
				},
			},
			want: &options{
				host:               "localhost",
				port:               50000,
				authToken:          "authToken",
				channel:            "test",
				autoAcknowledged:   true,
				pollMaxItems:       10,
				pollTimeoutSeconds: 10,
			},
			wantErr: false,
		},
		{
			name: "create invalid opts with bad host",
			meta: bindings.Metadata{
				Base: metadata.Base{
					Name: "kubemq",
					Properties: map[string]string{
						"address":  ":50000",
						"clientId": "clientId",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with bad port",
			meta: bindings.Metadata{
				Base: metadata.Base{
					Name: "kubemq",
					Properties: map[string]string{
						"address":  "localhost:badport",
						"clientId": "clientId",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with empty address",
			meta: bindings.Metadata{
				Base: metadata.Base{
					Name: "kubemq",
					Properties: map[string]string{
						"address":  "",
						"clientId": "clientId",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with bad address format",
			meta: bindings.Metadata{Base: metadata.Base{
				Name: "kubemq",
				Properties: map[string]string{
					"address": "localhost50000",
				},
			}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with no channel",
			meta: bindings.Metadata{Base: metadata.Base{
				Name: "kubemq",
				Properties: map[string]string{
					"address": "localhost:50000",
				},
			}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with bad autoAcknowledged",
			meta: bindings.Metadata{Base: metadata.Base{
				Name: "kubemq",
				Properties: map[string]string{
					"address":          "localhost:50000",
					"channel":          "test",
					"autoAcknowledged": "bad",
				},
			}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with invalid pollMaxItems",
			meta: bindings.Metadata{Base: metadata.Base{
				Name: "kubemq",
				Properties: map[string]string{
					"address":      "localhost:50000",
					"channel":      "test",
					"pollMaxItems": "0",
				},
			}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with bad pollMaxItems format",
			meta: bindings.Metadata{Base: metadata.Base{
				Name: "kubemq",
				Properties: map[string]string{
					"address":      "localhost:50000",
					"channel":      "test",
					"pollMaxItems": "bad",
				},
			}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with invalid pollTimeoutSeconds",
			meta: bindings.Metadata{Base: metadata.Base{
				Name: "kubemq",
				Properties: map[string]string{
					"address":            "localhost:50000",
					"channel":            "test",
					"pollTimeoutSeconds": "0",
				},
			}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "create invalid opts with bad format pollTimeoutSeconds",
			meta: bindings.Metadata{Base: metadata.Base{
				Name: "kubemq",
				Properties: map[string]string{
					"address":            "localhost:50000",
					"channel":            "test",
					"pollTimeoutSeconds": "bad",
				},
			}},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createOptions(tt.meta)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_parsePolicyDelaySeconds(t *testing.T) {
	type args struct {
		md map[string]string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "parse policy delay seconds - nil",
			args: args{
				md: nil,
			},
			want: 0,
		},
		{
			name: "parse policy delay seconds - empty",
			args: args{
				md: map[string]string{},
			},
			want: 0,
		},
		{
			name: "parse policy delay seconds",
			args: args{
				md: map[string]string{
					"delaySeconds": "10",
				},
			},
			want: 10,
		},
		{
			name: "parse policy delay seconds with bad format",
			args: args{
				md: map[string]string{
					"delaySeconds": "bad",
				},
			},
			want: 0,
		},
		{
			name: "parse policy delay seconds with negative value",
			args: args{
				md: map[string]string{
					"delaySeconds": "-10",
				},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, parsePolicyDelaySeconds(tt.args.md), "parsePolicyDelaySeconds(%v)", tt.args.md)
		})
	}
}

func Test_parsePolicyExpirationSeconds(t *testing.T) {
	type args struct {
		md map[string]string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "parse policy expiration seconds - nil",
			args: args{
				md: nil,
			},
			want: 0,
		},
		{
			name: "parse policy expiration seconds - empty",
			args: args{
				md: map[string]string{},
			},
			want: 0,
		},
		{
			name: "parse policy expiration seconds",
			args: args{
				md: map[string]string{
					"expirationSeconds": "10",
				},
			},
			want: 10,
		},
		{
			name: "parse policy expiration seconds with bad format",
			args: args{
				md: map[string]string{
					"expirationSeconds": "bad",
				},
			},
			want: 0,
		},
		{
			name: "parse policy expiration seconds with negative value",
			args: args{
				md: map[string]string{
					"expirationSeconds": "-10",
				},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, parsePolicyExpirationSeconds(tt.args.md), "parsePolicyExpirationSeconds(%v)", tt.args.md)
		})
	}
}

func Test_parseSetPolicyMaxReceiveCount(t *testing.T) {
	type args struct {
		md map[string]string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "parse policy max receive count nil",
			args: args{
				md: nil,
			},
			want: 0,
		},
		{
			name: "parse policy max receive count empty",
			args: args{
				md: map[string]string{},
			},
			want: 0,
		},
		{
			name: "parse policy max receive count",
			args: args{
				md: map[string]string{
					"maxReceiveCount": "10",
				},
			},
			want: 10,
		},

		{
			name: "parse policy max receive count with bad format",
			args: args{
				md: map[string]string{
					"maxReceiveCount": "bad",
				},
			},
			want: 0,
		},
		{
			name: "parse policy max receive count with negative value",
			args: args{
				md: map[string]string{
					"maxReceiveCount": "-10",
				},
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, parseSetPolicyMaxReceiveCount(tt.args.md), "parseSetPolicyMaxReceiveCount(%v)", tt.args.md)
		})
	}
}

func Test_parsePolicyMaxReceiveQueue(t *testing.T) {
	type args struct {
		md map[string]string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "parse policy max receive queue nil",
			args: args{
				md: nil,
			},
			want: "",
		},
		{
			name: "parse policy max receive queue empty",
			args: args{
				md: map[string]string{},
			},
			want: "",
		},
		{
			name: "parse policy max receive queue",
			args: args{
				md: map[string]string{
					"maxReceiveQueue": "some-queue",
				},
			},
			want: "some-queue",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, parsePolicyMaxReceiveQueue(tt.args.md), "parsePolicyMaxReceiveQueue(%v)", tt.args.md)
		})
	}
}
