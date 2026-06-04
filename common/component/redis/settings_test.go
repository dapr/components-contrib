package redis

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveHost(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		port    uint16
		want    string
		wantErr bool
	}{
		{
			name: "host:port already present",
			host: "redis-master:6379",
			port: 0,
			want: "redis-master:6379",
		},
		{
			name: "host:port matching separate port",
			host: "redis-master:6379",
			port: 6379,
			want: "redis-master:6379",
		},
		{
			name:    "host:port conflicts with separate port",
			host:    "redis-master:6379",
			port:    6380,
			wantErr: true,
		},
		{
			name: "host only with separate port",
			host: "redis-master",
			port: 6380,
			want: "redis-master:6380",
		},
		{
			name: "host only defaults to 6379",
			host: "redis-master",
			port: 0,
			want: "redis-master:6379",
		},
		{
			name: "empty host returns empty",
			host: "",
			port: 6379,
			want: "",
		},
		{
			name: "cluster hosts all with ports",
			host: "node1:6379,node2:6379,node3:6379",
			port: 0,
			want: "node1:6379,node2:6379,node3:6379",
		},
		{
			name: "cluster hosts without ports get the specified port applied",
			host: "node1,node2,node3",
			port: 6380,
			want: "node1:6380,node2:6380,node3:6380",
		},
		{
			name: "cluster hosts without ports default to 6379",
			host: "node1,node2,node3",
			port: 0,
			want: "node1:6379,node2:6379,node3:6379",
		},
		{
			name:    "cluster hosts one conflicts with separate port",
			host:    "node1:6379,node2,node3:6381",
			port:    6380,
			wantErr: true,
		},
		{
			name: "cluster hosts with spaces around commas",
			host: "node1 , node2 , node3",
			port: 6380,
			want: "node1:6380,node2:6380,node3:6380",
		},
		{
			name: "mixed cluster hosts: some with matching port, some without",
			host: "node1:6380,node2,node3:6380",
			port: 6380,
			want: "node1:6380,node2:6380,node3:6380",
		},
		{
			name: "mixed cluster hosts: some with port, some without, using default 6379",
			host: "node1:6379,node2,node3",
			port: 0,
			want: "node1:6379,node2:6379,node3:6379",
		},
		{
			name:    "mixed cluster hosts: one entry conflicts with redisPort",
			host:    "node1:6380,node2,node3:9999",
			port:    6380,
			wantErr: true,
		},
		{
			name: "sentinel addresses without ports",
			host: "sentinel1,sentinel2,sentinel3",
			port: 26379,
			want: "sentinel1:26379,sentinel2:26379,sentinel3:26379",
		},
		{
			name: "IPv6 address with port in brackets",
			host: "[::1]:6379",
			port: 0,
			want: "[::1]:6379",
		},
		{
			name:    "IPv6 address with port conflicts with separate port",
			host:    "[::1]:6379",
			port:    6380,
			wantErr: true,
		},
		{
			name: "FQDN host without port",
			host: "redis.staging.example.com",
			port: 6379,
			want: "redis.staging.example.com:6379",
		},
		{
			name: "valid port: upper boundary",
			host: "redis-master",
			port: 65535,
			want: "redis-master:65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveHost(tt.host, tt.port)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "redisPort")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestSettingsDecodeResolvesHostPort(t *testing.T) {
	t.Run("decode with separate port", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "redis-master",
			"redisPort": "6380",
		})
		require.NoError(t, err)
		require.Equal(t, "redis-master:6380", s.Host)
	})

	t.Run("decode with host:port matching separate port", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "redis-master:6379",
			"redisPort": "6379",
		})
		require.NoError(t, err)
		require.Equal(t, "redis-master:6379", s.Host)
	})

	t.Run("decode with host:port conflicting separate port errors", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "redis-master:6379",
			"redisPort": "6380",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "redisPort")
	})

	t.Run("decode with host only defaults port", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "redis-master",
		})
		require.NoError(t, err)
		require.Equal(t, "redis-master:6379", s.Host)
	})

	t.Run("decode cluster hosts with separate port", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "node1,node2,node3",
			"redisPort": "6380",
		})
		require.NoError(t, err)
		require.Equal(t, "node1:6380,node2:6380,node3:6380", s.Host)
	})

	t.Run("decode cluster host conflict errors", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "node1:6379,node2,node3",
			"redisPort": "6380",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "redisPort")
	})

	t.Run("decode with invalid port rejects at decode", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "redis-master",
			"redisPort": "abc",
		})
		require.Error(t, err)
	})

	t.Run("decode with out-of-range port rejects at decode", func(t *testing.T) {
		s := Settings{}
		err := s.Decode(map[string]string{
			"redisHost": "redis-master",
			"redisPort": "99999",
		})
		require.Error(t, err)
	})
}

func TestSettings(t *testing.T) {
	t.Run("test set certificate, missing data", func(t *testing.T) {
		var c *tls.Certificate
		settings := &Settings{}
		err := settings.SetCertificate(func(cert *tls.Certificate) {
			c = cert
		})
		require.NoError(t, err)
		require.Nil(t, c)
	})

	t.Run("test set certificate, invalid data", func(t *testing.T) {
		settings := &Settings{
			ClientCert: "foo",
			ClientKey:  "bar",
		}
		err := settings.SetCertificate(nil)
		require.Error(t, err)
	})

	t.Run("test set certificate, valid data", func(t *testing.T) {
		var c *tls.Certificate
		settings := &Settings{
			ClientCert: "-----BEGIN CERTIFICATE-----\nMIIC+jCCAeKgAwIBAgIUcVW+K5LM+rLj80F0XWG0YXh6Hq4wDQYJKoZIhvcNAQEL\nBQAwFDESMBAGA1UEAwwJTXlSZWRpc0NBMB4XDTI0MDUyNzExMzQyMloXDTI1MDUy\nNzExMzQyMlowFjEUMBIGA1UEAwwLUmVkaXNDbGllbnQwggEiMA0GCSqGSIb3DQEB\nAQUAA4IBDwAwggEKAoIBAQCpSQKejofOA42jBSsfDVE5FdSxGEU+ktpqcp2CBZ8Z\nD9YLW4H6JTMU2JzPQLrwd5oF+FBdVDYkpunFs8lPGlvR7KMzXv130PSJ4ieSAEwJ\n7ocxKvqYpYmyFsPUHHOVJEYxlUK0nd8KvBw7OKbdk5tL/gEDoHKHJOZpiDmcMFqw\nlMfNrGGlsgZjcWvnZfEa3Q4D7hD3iNYJbLT9ETZZF36V5I8sXrexnlzN4EXyCZuF\nV9M/+5V+JwYamvpHrTiCR9oDVrHJytjSyvyysW7PKmLjs9C12opo6LBHoKEidCKV\nNyicgBfkvvHnlRDaANmELJpX5vNuW9lsEG+Rxiyf47rtAgMBAAGjQjBAMB0GA1Ud\nDgQWBBQal6ypaK/1V0SGwfLefKrIUkl2jTAfBgNVHSMEGDAWgBQ4NNFfx71nrJ19\nF9/rtg8TAqZbjjANBgkqhkiG9w0BAQsFAAOCAQEAEqN0Ja31Tkv6oHp35ksFFM2X\npej6BljJH61X4jIGJ7qFacG8OxkpA3mspF8NZx4SG1NeZVC5eYMixxqDDDxz5cli\nLVaxP9T3TiIU/lYpqnGBTaKJ6q4ngwTSTdZ9Xp1cVhYx80F9SK3l3TeLAUCVl/HK\neepV2BOfr/B/sK1gVTOcmxRHh8piPEcY49WCDA6+zd6UYXiaYtegMAomaoPA76Kf\nNRcNkWrm5sbyRKijw2AmjRxFGH2lTdCGg1CNXwQhCPrsGQKuDh5JPN/wqxF2GjSK\ncpuArGhwuCLf4kLoEB+0VgFwaKqrUwsWyD9P+vBh5kNf76B+NkBtp+19AzSasw==\n-----END CERTIFICATE-----",
			ClientKey:  "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCpSQKejofOA42j\nBSsfDVE5FdSxGEU+ktpqcp2CBZ8ZD9YLW4H6JTMU2JzPQLrwd5oF+FBdVDYkpunF\ns8lPGlvR7KMzXv130PSJ4ieSAEwJ7ocxKvqYpYmyFsPUHHOVJEYxlUK0nd8KvBw7\nOKbdk5tL/gEDoHKHJOZpiDmcMFqwlMfNrGGlsgZjcWvnZfEa3Q4D7hD3iNYJbLT9\nETZZF36V5I8sXrexnlzN4EXyCZuFV9M/+5V+JwYamvpHrTiCR9oDVrHJytjSyvyy\nsW7PKmLjs9C12opo6LBHoKEidCKVNyicgBfkvvHnlRDaANmELJpX5vNuW9lsEG+R\nxiyf47rtAgMBAAECggEAChFC/B362pgggLzqcxbOKUhwlTWdzJpcLe9yCYz/CLUF\n5DgFc1RqBMfbD4JIe8uJF+jMErjS3Xwls/G8u50UL9hUXlY8WbdOC7Ms6kRlQUPz\nu0tUiuZxWWt8Ku2kPA7js8guJuKqpI9KWIVGey/vkOXito4AsaPSph0JXA4OHqkp\nqdT6Q+ZIiSoCopaubmcToYr28g/g2K37IPDRaStCMEm2KFEL+D1VzCfucKgXrZdK\np9M9e5WoUR4J9p4yxNAwafXvdRxZq40SRJTB+3x172Epg2vhVJLAs0b8ArI9z57+\nOodWfCiebfQQ4EKIZLY0Djr5kYRv1yrwKjViktv4WwKBgQDcTB2KILsDSw6KP+c5\nzNm6WLP6SQ8mlXj5r0VWDDfKqo7w8UTzU+PMB1rDDmjLV0c7PKFadB6LsMlXUoKr\n+/iRoea9UNOwdy9JQ2DesjCFWbu2KigOO06X2LxckHBUNe0VbvCVXZ9hz3d8PkpA\n/Ib8m55Q2uY/iM3jtufF9XT5qwKBgQDEuHW2MXp7Kt9+3EAmy9yBggEkX+4MjhKM\ntPSLNWnxOz6ZiF0KZfwdZ1TmQxBJ0CPSoKuf6DrdFY6Rm2ZLkDKhRdw9LkN2hvne\nnIkteVSLzok0a1ryHYENXRRUOVN6s3E7X+b/uFiR7bemx/Tzrpc2yBJk3gJm+ffP\nVbFjug/1xwKBgQDats8VFg3V5SzYYT2GCzWXZv243dQm8HudGUBzf8nccp1b5Y4Z\nLw6YwCyCP8oXJ93WmAlyLpstASXEhmypp45PuDfHeXnSV2IhEL4aGztFCaPt5cjC\n6GrNIydPly+Oy8NIZk6BXOQiTcJJHebGwnCaVz5E9C9ooMAY9r0BswKh5QKBgFGt\nsRo7wvoe2/slYfF51Y1kOCstNX67ApKvk5W1UM6bZauDxfXKUHq467RLhhjPtf//\nPCNB3ibri22Dk16ueYcipYY1jkdJVbgLUJ2z8dm2oJtGM9WxUGMHEajCwJmCpfIc\nKKJmnUfB5u31ugvvotNZEOIWl/K/uRe6IdQhbf0DAoGAdw7GEmzs6Rdq4LTs9cfQ\nQaXtx8vAXAU5X0StEtJWIlDnRbEvV8NaiPEwXfcuvx+Y1nvPSXuaq+YhxgrlBviL\nuKt2V2ONIrUsfRuPePSHjUipMR92A/8hPhOxw2SCYemtzniYJbeulUhIAWbQplhT\nIeadrRMdaWmKA1OGeIZRRw4=\n-----END PRIVATE KEY-----",
		}
		err := settings.SetCertificate(func(cert *tls.Certificate) {
			c = cert
		})
		require.NoError(t, err)
		require.NotNil(t, c)
	})

	t.Run("stream TTL", func(t *testing.T) {
		fixedTime := time.Date(2025, 3, 14, 0o1, 59, 26, 0, time.UTC)

		tests := []struct {
			name      string
			streamTTL time.Duration
			want      string
		}{
			{
				name:      "with one hour TTL",
				streamTTL: time.Hour,
				want:      "1741913966000-1",
			},
			{
				name:      "with zero TTL",
				streamTTL: 0,
				want:      "",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				settings := &Settings{
					StreamTTL: tt.streamTTL,
				}
				require.Equal(t, tt.want, settings.GetMinID(fixedTime))
			})
		}
	})
}
