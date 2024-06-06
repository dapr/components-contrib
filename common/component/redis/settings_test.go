package redis

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

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
}
