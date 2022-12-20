package pubsub

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertTLSPropertiesToTLSConfig(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		fakeProperties := TLSProperties{}
		fakeProperties.ClientCert = "-----BEGIN CERTIFICATE-----\nMIIEZjCCA06gAwIBAgIJAMQ5Az0QUDY+MA0GCSqGSIb3DQEBCwUAMGwxCzAJBgNV\nBAYTAlVTMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2Fs\naXR5MRUwEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRN\nUVRlc3QwHhcNMjIxMTEyMTMyNjEyWhcNMjUwMjE0MTMyNjEyWjBsMQswCQYDVQQG\nEwJVUzETMBEGA1UECAwKRmFrZSBTdGF0ZTEWMBQGA1UEBwwNRmFrZSBMb2NhbGl0\neTEVMBMGA1UECgwMRmFrZSBDb21wYW55MRkwFwYDVQQDDBBkYXByUmFiYml0TVFU\nZXN0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqNwAxLCx5J367N41\net6SLa0aWGLHeU61jTz0VG6tKsLPEw3wD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rb\nq9cwHBPbNxRksFQQZPOBONLkcs1yijjMExyc2GsMLCwUFKZWD73SEZe1Hace9otG\n1FHjQgHs2bYIOckHHGQIampM/5L931A9M6j5JHenF4m7KBIDCCAhvka5fuGFsLIs\nke5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQwQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM\n6uobZ7pWU95S8XEbrRygAKPCXmoVWxfsKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek\n05BgAwIDAQABo4IBCTCCAQUwgYYGA1UdIwR/MH2hcKRuMGwxCzAJBgNVBAYTAlVT\nMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2FsaXR5MRUw\nEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRNUVRlc3SC\nCQCpmf4h1/pxHTAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAxBgNVHSUEKjAoBggr\nBgEFBQcDAQYIKwYBBQUHAwIGCCsGAQUFBwMDBggrBgEFBQcDBDAvBgNVHREEKDAm\ngglsb2NhbGhvc3SCCTEyNy4wLjAuMYIDOjoxgglsb2NhbC5kZXYwDQYJKoZIhvcN\nAQELBQADggEBAGhAqIEzj5cOr0GRf6uhkx3s2s5WGWJlb+J6J2mam/Zu8Z5olFj+\nOWzCwfw/ZV8q5Domr6ddmgOMz+URrckst86/fh597/uv42KwQt/bBmZCvTrr+QjM\nxDmhCTIF8aRl54DQxIZpPBhvBG1fg9E1NGa426zNuySVz/A10aAPlZ1D94iwHOvR\n9UXDG9JVhYYbrgGKloWog+U8viqzLMFeRyMhp4JL1FbGTq/+2FpYD7nc6xq8nm2G\nvAEJ4Tw1exbJc+fcRXUUrxRXTHxJEThRHycXyMZgIZsIHSYGeQOH6HOwp/t+/IyB\n93KPobjIt25cwepLlRWHsGnjFOu/gulXQ3w=\n-----END CERTIFICATE-----"
		fakeProperties.ClientKey = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpQIBAAKCAQEAqNwAxLCx5J367N41et6SLa0aWGLHeU61jTz0VG6tKsLPEw3w\nD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rbq9cwHBPbNxRksFQQZPOBONLkcs1yijjM\nExyc2GsMLCwUFKZWD73SEZe1Hace9otG1FHjQgHs2bYIOckHHGQIampM/5L931A9\nM6j5JHenF4m7KBIDCCAhvka5fuGFsLIske5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQw\nQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM6uobZ7pWU95S8XEbrRygAKPCXmoVWxfs\nKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek05BgAwIDAQABAoIBAQCHYTNgfoVxVFN8\nGr9nzhAGfNU1baSdlkQqg2ckVkORl/QrQ65m/HfXow6wF5l6LrRv2Qz8Z3LwdXZa\n+9g/Ulwo9qZ3Z2s+j3dBaJT+uN4dSKf/N4AuBm+dd12gAIrs71rqbfaA0k+MAZPq\neEmGKkz4e9Cnz7CSm6CO24h/wNAQyjwi+/QBxKVix5+BKgMK2AQd/xmlzbGxGO6T\n0UTRCbx6CaitX7I6sLU8C9ebcsB2lO/y+VDPeVU/ij0zLQJLCY0px/TmRrcak+WD\n/POnL3JSr6AqvGoOhYkSTkzijEjHMZwZM4pakoI5vSGWDIxmb8GpnmwTjRy5R1e6\nTShbO4bxAoGBANZlfLZVsLGHN3o4OShVjI1qJomHIEFsbeT39KtCNIwwDZuoOWB7\nH4ENwbLBH4kAWOqvdoqxhsic0RHFsSaxn4ubA9q1XA1oLzu2HlAiBG4hSUhclXI8\nzWREXYmyCgKXrje9gMn3g+cfTFIFdJSLcQgaBRVFNuuj/z8BTHCIK1gLAoGBAMmg\nYmnzsRncuwREuOGmfu+NtyO3e3tMacce5DxsAZzYBdnmxUnfPUbqdivSfM8ls9gD\nXoQnqkfA7lK/kk2KgYFzHulSlXhVUoMzQMJR86DcMtyUXw6Y4TShXoxmcH6fd4bb\ndFanPGnqF7+rrPnvrLlbJavce8Yv99HghVhFeHrpAoGBAJLvHOiNk7ondIMD01Bs\nSRaGAIFSpW2RFUPOF9XjWsYVDM54HVKdLzswJWcP6j/niAjXVgO5pSkKKFLozP86\nzqBMDfkvQDZEB9MBfobmuWiM0X+RTC7tssob/IspBKlAMPssmf5Q7wpQLessp/hC\nFKY7xu4L8JbQX1db2XpqKGJ/AoGBAKCFX9SaftToNrwfOlHsDAMMAFIfMd/n5g7x\nOSQhmOcV1RK19zvozxA2ef5JhbQlgBjqFcWBDsAxiYu3mPLC4nf8/w8jAHzc8OQj\nWdtbA2t948BZH5Svghw0nOGKbOVib/adoPGcEyz/ggjG1N/nQMwRFNzOnHwclGAz\nL/Ym2NSBAoGAOQW6/CQhLCS3qyh89NzQSL8FUEYskOSthxqnRJyCDpNtVR4JGoG7\nSqjY4tMCm30X4qJuBUuhymtdCDRMKfcBEQ1RcfQgW/DZEsOk5OqobF84ve8eJ89F\nWZuSgACcvoYumRQ8+fp4nQ74EQloOkKmvWTUbguCZLEnecpYkzw/RhU=\n-----END RSA PRIVATE KEY-----\n"
		fakeProperties.CACert = "-----BEGIN CERTIFICATE-----\nMIIDVDCCAjwCCQCpmf4h1/pxHTANBgkqhkiG9w0BAQsFADBsMQswCQYDVQQGEwJV\nUzETMBEGA1UECAwKRmFrZSBTdGF0ZTEWMBQGA1UEBwwNRmFrZSBMb2NhbGl0eTEV\nMBMGA1UECgwMRmFrZSBDb21wYW55MRkwFwYDVQQDDBBkYXByUmFiYml0TVFUZXN0\nMB4XDTIyMTExMjEzMjU1MVoXDTI1MDIxNDEzMjU1MVowbDELMAkGA1UEBhMCVVMx\nEzARBgNVBAgMCkZha2UgU3RhdGUxFjAUBgNVBAcMDUZha2UgTG9jYWxpdHkxFTAT\nBgNVBAoMDEZha2UgQ29tcGFueTEZMBcGA1UEAwwQZGFwclJhYmJpdE1RVGVzdDCC\nASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL1rXcV0uN2UGaSizscS8Sav\n5ifZRXTGLx8vUpt7fDGgO9zZpZrTluduK7ReZrHN/5cML0TQuHR6CE5bi4L4PLc+\nH4Ir2i2aDXQ7kTfxFEK/M9q57nKTepu7Wu1u0MDdpLzB83huTDPX0AksDR+8e4cT\nLmxMJ0EkifEJrBdffLPoYKsdG9Fdrk3KS6NWFEIIAamCNRhrMX1DsEd2yOsOzbhK\nX1m5/g9jyDdaZYUOb2j7li8b0D+PZMKNukaEwZt7OAy8vkgOXup/H5Jq4RKUxiOi\nPczk0xVAl5i+cLpcAHBl8nL/ryMosHQZujEqIU5buy6aRDHY8PmZbvmYqbhFjEkC\nAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAsebNZV6B4OoO5Tf/NiGHotYrny69FoHH\ncSULpGShl3PFJnLzeMaq80R2v/pQ3EH1SjO0k0dcprNsJ35JJwBjLrV1n7ZsMVZp\n2i/2WkPiOuGncnwq4LioAl2rm2GtgBTua0WHey1dAUUtg7qG2yxsXCzBVXL/rDTv\nzdADW+IiFW81FusIs3WbeMXZxNyWZD9tfLsqjSxVqBn6ER4+9rrWCxEOPoApE8IY\nAp6GgG3wlCr2IheBgL4QI1FaYl/ZAXAlzh0IS1X1HUjX+pKJ0nboNF8H1XTT5FSN\n8FsHHG+vEsXg6/In7v/F1akOSF63PDhjAL696ouwnbOj5jRUx3zYUQ==\n-----END CERTIFICATE-----\n"

		// act
		c, err := ConvertTLSPropertiesToTLSConfig(fakeProperties)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, len(c.Certificates), 1)
		assert.NotNil(t, c.RootCAs)
	})

	t.Run("empty properties", func(t *testing.T) {
		fakeProperties := TLSProperties{}

		// act
		c, err := ConvertTLSPropertiesToTLSConfig(fakeProperties)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, len(c.Certificates), 0)
		assert.Nil(t, c.RootCAs)
	})

	t.Run("invalid client certificate and key pair", func(t *testing.T) {
		fakeProperties := TLSProperties{}
		fakeProperties.ClientKey = "randomClientKey"
		fakeProperties.ClientCert = "randomClientCert"

		_, err := ConvertTLSPropertiesToTLSConfig(fakeProperties)

		// assert
		assert.Contains(t, err.Error(), "unable to load client certificate and key pair")
	})

	t.Run("invalid ca certificate", func(t *testing.T) {
		fakeProperties := TLSProperties{}
		fakeProperties.ClientCert = "-----BEGIN CERTIFICATE-----\nMIIEZjCCA06gAwIBAgIJAMQ5Az0QUDY+MA0GCSqGSIb3DQEBCwUAMGwxCzAJBgNV\nBAYTAlVTMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2Fs\naXR5MRUwEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRN\nUVRlc3QwHhcNMjIxMTEyMTMyNjEyWhcNMjUwMjE0MTMyNjEyWjBsMQswCQYDVQQG\nEwJVUzETMBEGA1UECAwKRmFrZSBTdGF0ZTEWMBQGA1UEBwwNRmFrZSBMb2NhbGl0\neTEVMBMGA1UECgwMRmFrZSBDb21wYW55MRkwFwYDVQQDDBBkYXByUmFiYml0TVFU\nZXN0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqNwAxLCx5J367N41\net6SLa0aWGLHeU61jTz0VG6tKsLPEw3wD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rb\nq9cwHBPbNxRksFQQZPOBONLkcs1yijjMExyc2GsMLCwUFKZWD73SEZe1Hace9otG\n1FHjQgHs2bYIOckHHGQIampM/5L931A9M6j5JHenF4m7KBIDCCAhvka5fuGFsLIs\nke5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQwQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM\n6uobZ7pWU95S8XEbrRygAKPCXmoVWxfsKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek\n05BgAwIDAQABo4IBCTCCAQUwgYYGA1UdIwR/MH2hcKRuMGwxCzAJBgNVBAYTAlVT\nMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2FsaXR5MRUw\nEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRNUVRlc3SC\nCQCpmf4h1/pxHTAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAxBgNVHSUEKjAoBggr\nBgEFBQcDAQYIKwYBBQUHAwIGCCsGAQUFBwMDBggrBgEFBQcDBDAvBgNVHREEKDAm\ngglsb2NhbGhvc3SCCTEyNy4wLjAuMYIDOjoxgglsb2NhbC5kZXYwDQYJKoZIhvcN\nAQELBQADggEBAGhAqIEzj5cOr0GRf6uhkx3s2s5WGWJlb+J6J2mam/Zu8Z5olFj+\nOWzCwfw/ZV8q5Domr6ddmgOMz+URrckst86/fh597/uv42KwQt/bBmZCvTrr+QjM\nxDmhCTIF8aRl54DQxIZpPBhvBG1fg9E1NGa426zNuySVz/A10aAPlZ1D94iwHOvR\n9UXDG9JVhYYbrgGKloWog+U8viqzLMFeRyMhp4JL1FbGTq/+2FpYD7nc6xq8nm2G\nvAEJ4Tw1exbJc+fcRXUUrxRXTHxJEThRHycXyMZgIZsIHSYGeQOH6HOwp/t+/IyB\n93KPobjIt25cwepLlRWHsGnjFOu/gulXQ3w=\n-----END CERTIFICATE-----"
		fakeProperties.ClientKey = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpQIBAAKCAQEAqNwAxLCx5J367N41et6SLa0aWGLHeU61jTz0VG6tKsLPEw3w\nD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rbq9cwHBPbNxRksFQQZPOBONLkcs1yijjM\nExyc2GsMLCwUFKZWD73SEZe1Hace9otG1FHjQgHs2bYIOckHHGQIampM/5L931A9\nM6j5JHenF4m7KBIDCCAhvka5fuGFsLIske5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQw\nQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM6uobZ7pWU95S8XEbrRygAKPCXmoVWxfs\nKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek05BgAwIDAQABAoIBAQCHYTNgfoVxVFN8\nGr9nzhAGfNU1baSdlkQqg2ckVkORl/QrQ65m/HfXow6wF5l6LrRv2Qz8Z3LwdXZa\n+9g/Ulwo9qZ3Z2s+j3dBaJT+uN4dSKf/N4AuBm+dd12gAIrs71rqbfaA0k+MAZPq\neEmGKkz4e9Cnz7CSm6CO24h/wNAQyjwi+/QBxKVix5+BKgMK2AQd/xmlzbGxGO6T\n0UTRCbx6CaitX7I6sLU8C9ebcsB2lO/y+VDPeVU/ij0zLQJLCY0px/TmRrcak+WD\n/POnL3JSr6AqvGoOhYkSTkzijEjHMZwZM4pakoI5vSGWDIxmb8GpnmwTjRy5R1e6\nTShbO4bxAoGBANZlfLZVsLGHN3o4OShVjI1qJomHIEFsbeT39KtCNIwwDZuoOWB7\nH4ENwbLBH4kAWOqvdoqxhsic0RHFsSaxn4ubA9q1XA1oLzu2HlAiBG4hSUhclXI8\nzWREXYmyCgKXrje9gMn3g+cfTFIFdJSLcQgaBRVFNuuj/z8BTHCIK1gLAoGBAMmg\nYmnzsRncuwREuOGmfu+NtyO3e3tMacce5DxsAZzYBdnmxUnfPUbqdivSfM8ls9gD\nXoQnqkfA7lK/kk2KgYFzHulSlXhVUoMzQMJR86DcMtyUXw6Y4TShXoxmcH6fd4bb\ndFanPGnqF7+rrPnvrLlbJavce8Yv99HghVhFeHrpAoGBAJLvHOiNk7ondIMD01Bs\nSRaGAIFSpW2RFUPOF9XjWsYVDM54HVKdLzswJWcP6j/niAjXVgO5pSkKKFLozP86\nzqBMDfkvQDZEB9MBfobmuWiM0X+RTC7tssob/IspBKlAMPssmf5Q7wpQLessp/hC\nFKY7xu4L8JbQX1db2XpqKGJ/AoGBAKCFX9SaftToNrwfOlHsDAMMAFIfMd/n5g7x\nOSQhmOcV1RK19zvozxA2ef5JhbQlgBjqFcWBDsAxiYu3mPLC4nf8/w8jAHzc8OQj\nWdtbA2t948BZH5Svghw0nOGKbOVib/adoPGcEyz/ggjG1N/nQMwRFNzOnHwclGAz\nL/Ym2NSBAoGAOQW6/CQhLCS3qyh89NzQSL8FUEYskOSthxqnRJyCDpNtVR4JGoG7\nSqjY4tMCm30X4qJuBUuhymtdCDRMKfcBEQ1RcfQgW/DZEsOk5OqobF84ve8eJ89F\nWZuSgACcvoYumRQ8+fp4nQ74EQloOkKmvWTUbguCZLEnecpYkzw/RhU=\n-----END RSA PRIVATE KEY-----\n"
		fakeProperties.CACert = "randomCACertificate"

		_, err := ConvertTLSPropertiesToTLSConfig(fakeProperties)

		// assert
		assert.Contains(t, err.Error(), "unable to load CA certificate")
	})
}

func TestTLS(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		fakeProperties := map[string]string{}
		fakeProperties[ClientCert] = "-----BEGIN CERTIFICATE-----\nMIIEZjCCA06gAwIBAgIJAMQ5Az0QUDY+MA0GCSqGSIb3DQEBCwUAMGwxCzAJBgNV\nBAYTAlVTMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2Fs\naXR5MRUwEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRN\nUVRlc3QwHhcNMjIxMTEyMTMyNjEyWhcNMjUwMjE0MTMyNjEyWjBsMQswCQYDVQQG\nEwJVUzETMBEGA1UECAwKRmFrZSBTdGF0ZTEWMBQGA1UEBwwNRmFrZSBMb2NhbGl0\neTEVMBMGA1UECgwMRmFrZSBDb21wYW55MRkwFwYDVQQDDBBkYXByUmFiYml0TVFU\nZXN0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqNwAxLCx5J367N41\net6SLa0aWGLHeU61jTz0VG6tKsLPEw3wD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rb\nq9cwHBPbNxRksFQQZPOBONLkcs1yijjMExyc2GsMLCwUFKZWD73SEZe1Hace9otG\n1FHjQgHs2bYIOckHHGQIampM/5L931A9M6j5JHenF4m7KBIDCCAhvka5fuGFsLIs\nke5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQwQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM\n6uobZ7pWU95S8XEbrRygAKPCXmoVWxfsKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek\n05BgAwIDAQABo4IBCTCCAQUwgYYGA1UdIwR/MH2hcKRuMGwxCzAJBgNVBAYTAlVT\nMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2FsaXR5MRUw\nEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRNUVRlc3SC\nCQCpmf4h1/pxHTAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAxBgNVHSUEKjAoBggr\nBgEFBQcDAQYIKwYBBQUHAwIGCCsGAQUFBwMDBggrBgEFBQcDBDAvBgNVHREEKDAm\ngglsb2NhbGhvc3SCCTEyNy4wLjAuMYIDOjoxgglsb2NhbC5kZXYwDQYJKoZIhvcN\nAQELBQADggEBAGhAqIEzj5cOr0GRf6uhkx3s2s5WGWJlb+J6J2mam/Zu8Z5olFj+\nOWzCwfw/ZV8q5Domr6ddmgOMz+URrckst86/fh597/uv42KwQt/bBmZCvTrr+QjM\nxDmhCTIF8aRl54DQxIZpPBhvBG1fg9E1NGa426zNuySVz/A10aAPlZ1D94iwHOvR\n9UXDG9JVhYYbrgGKloWog+U8viqzLMFeRyMhp4JL1FbGTq/+2FpYD7nc6xq8nm2G\nvAEJ4Tw1exbJc+fcRXUUrxRXTHxJEThRHycXyMZgIZsIHSYGeQOH6HOwp/t+/IyB\n93KPobjIt25cwepLlRWHsGnjFOu/gulXQ3w=\n-----END CERTIFICATE-----"
		fakeProperties[CACert] = "-----BEGIN CERTIFICATE-----\nMIIDVDCCAjwCCQCpmf4h1/pxHTANBgkqhkiG9w0BAQsFADBsMQswCQYDVQQGEwJV\nUzETMBEGA1UECAwKRmFrZSBTdGF0ZTEWMBQGA1UEBwwNRmFrZSBMb2NhbGl0eTEV\nMBMGA1UECgwMRmFrZSBDb21wYW55MRkwFwYDVQQDDBBkYXByUmFiYml0TVFUZXN0\nMB4XDTIyMTExMjEzMjU1MVoXDTI1MDIxNDEzMjU1MVowbDELMAkGA1UEBhMCVVMx\nEzARBgNVBAgMCkZha2UgU3RhdGUxFjAUBgNVBAcMDUZha2UgTG9jYWxpdHkxFTAT\nBgNVBAoMDEZha2UgQ29tcGFueTEZMBcGA1UEAwwQZGFwclJhYmJpdE1RVGVzdDCC\nASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL1rXcV0uN2UGaSizscS8Sav\n5ifZRXTGLx8vUpt7fDGgO9zZpZrTluduK7ReZrHN/5cML0TQuHR6CE5bi4L4PLc+\nH4Ir2i2aDXQ7kTfxFEK/M9q57nKTepu7Wu1u0MDdpLzB83huTDPX0AksDR+8e4cT\nLmxMJ0EkifEJrBdffLPoYKsdG9Fdrk3KS6NWFEIIAamCNRhrMX1DsEd2yOsOzbhK\nX1m5/g9jyDdaZYUOb2j7li8b0D+PZMKNukaEwZt7OAy8vkgOXup/H5Jq4RKUxiOi\nPczk0xVAl5i+cLpcAHBl8nL/ryMosHQZujEqIU5buy6aRDHY8PmZbvmYqbhFjEkC\nAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAsebNZV6B4OoO5Tf/NiGHotYrny69FoHH\ncSULpGShl3PFJnLzeMaq80R2v/pQ3EH1SjO0k0dcprNsJ35JJwBjLrV1n7ZsMVZp\n2i/2WkPiOuGncnwq4LioAl2rm2GtgBTua0WHey1dAUUtg7qG2yxsXCzBVXL/rDTv\nzdADW+IiFW81FusIs3WbeMXZxNyWZD9tfLsqjSxVqBn6ER4+9rrWCxEOPoApE8IY\nAp6GgG3wlCr2IheBgL4QI1FaYl/ZAXAlzh0IS1X1HUjX+pKJ0nboNF8H1XTT5FSN\n8FsHHG+vEsXg6/In7v/F1akOSF63PDhjAL696ouwnbOj5jRUx3zYUQ==\n-----END CERTIFICATE-----\n"
		fakeProperties[ClientKey] = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpQIBAAKCAQEAqNwAxLCx5J367N41et6SLa0aWGLHeU61jTz0VG6tKsLPEw3w\nD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rbq9cwHBPbNxRksFQQZPOBONLkcs1yijjM\nExyc2GsMLCwUFKZWD73SEZe1Hace9otG1FHjQgHs2bYIOckHHGQIampM/5L931A9\nM6j5JHenF4m7KBIDCCAhvka5fuGFsLIske5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQw\nQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM6uobZ7pWU95S8XEbrRygAKPCXmoVWxfs\nKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek05BgAwIDAQABAoIBAQCHYTNgfoVxVFN8\nGr9nzhAGfNU1baSdlkQqg2ckVkORl/QrQ65m/HfXow6wF5l6LrRv2Qz8Z3LwdXZa\n+9g/Ulwo9qZ3Z2s+j3dBaJT+uN4dSKf/N4AuBm+dd12gAIrs71rqbfaA0k+MAZPq\neEmGKkz4e9Cnz7CSm6CO24h/wNAQyjwi+/QBxKVix5+BKgMK2AQd/xmlzbGxGO6T\n0UTRCbx6CaitX7I6sLU8C9ebcsB2lO/y+VDPeVU/ij0zLQJLCY0px/TmRrcak+WD\n/POnL3JSr6AqvGoOhYkSTkzijEjHMZwZM4pakoI5vSGWDIxmb8GpnmwTjRy5R1e6\nTShbO4bxAoGBANZlfLZVsLGHN3o4OShVjI1qJomHIEFsbeT39KtCNIwwDZuoOWB7\nH4ENwbLBH4kAWOqvdoqxhsic0RHFsSaxn4ubA9q1XA1oLzu2HlAiBG4hSUhclXI8\nzWREXYmyCgKXrje9gMn3g+cfTFIFdJSLcQgaBRVFNuuj/z8BTHCIK1gLAoGBAMmg\nYmnzsRncuwREuOGmfu+NtyO3e3tMacce5DxsAZzYBdnmxUnfPUbqdivSfM8ls9gD\nXoQnqkfA7lK/kk2KgYFzHulSlXhVUoMzQMJR86DcMtyUXw6Y4TShXoxmcH6fd4bb\ndFanPGnqF7+rrPnvrLlbJavce8Yv99HghVhFeHrpAoGBAJLvHOiNk7ondIMD01Bs\nSRaGAIFSpW2RFUPOF9XjWsYVDM54HVKdLzswJWcP6j/niAjXVgO5pSkKKFLozP86\nzqBMDfkvQDZEB9MBfobmuWiM0X+RTC7tssob/IspBKlAMPssmf5Q7wpQLessp/hC\nFKY7xu4L8JbQX1db2XpqKGJ/AoGBAKCFX9SaftToNrwfOlHsDAMMAFIfMd/n5g7x\nOSQhmOcV1RK19zvozxA2ef5JhbQlgBjqFcWBDsAxiYu3mPLC4nf8/w8jAHzc8OQj\nWdtbA2t948BZH5Svghw0nOGKbOVib/adoPGcEyz/ggjG1N/nQMwRFNzOnHwclGAz\nL/Ym2NSBAoGAOQW6/CQhLCS3qyh89NzQSL8FUEYskOSthxqnRJyCDpNtVR4JGoG7\nSqjY4tMCm30X4qJuBUuhymtdCDRMKfcBEQ1RcfQgW/DZEsOk5OqobF84ve8eJ89F\nWZuSgACcvoYumRQ8+fp4nQ74EQloOkKmvWTUbguCZLEnecpYkzw/RhU=\n-----END RSA PRIVATE KEY-----\n"

		// act
		p, err := TLS(fakeProperties)

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, p.ClientKey, "failed to parse valid client certificate key")
		block, _ := pem.Decode([]byte(p.ClientCert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse client certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprRabbitMQTest", cert.Subject.CommonName)

		block, _ = pem.Decode([]byte(p.CACert))
		cert, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse ca certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprRabbitMQTest", cert.Subject.CommonName)
	})

	t.Run("invalid client key", func(t *testing.T) {
		fakeProperties := map[string]string{}
		fakeProperties[ClientKey] = "randomClientKey"

		_, err := TLS(fakeProperties)

		// assert
		assert.Contains(t, err.Error(), "invalid clientKey")
	})

	t.Run("invalid client certificate", func(t *testing.T) {
		fakeProperties := map[string]string{}
		fakeProperties[ClientCert] = "randomClientCert"

		_, err := TLS(fakeProperties)

		// assert
		assert.Contains(t, err.Error(), "invalid clientCert")
	})

	t.Run("invalid ca certificate", func(t *testing.T) {
		fakeProperties := map[string]string{}
		fakeProperties[CACert] = "randomCACertificate"

		_, err := TLS(fakeProperties)

		// assert
		assert.Contains(t, err.Error(), "invalid caCert")
	})
}
