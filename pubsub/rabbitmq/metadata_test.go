/*
Copyright 2021 The Dapr Authors
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

package rabbitmq

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

func getFakeProperties() map[string]string {
	props := map[string]string{}
	props[metadataConnectionStringKey] = "amqps://localhost:5671"
	props[metadataProtocolKey] = "amqps"
	props[metadataHostnameKey] = "fakehostname"
	props[metadataUsernameKey] = "fakeusername"
	props[metadataPasswordKey] = "fakepassword"
	props[metadataConsumerIDKey] = "fakeConsumerID"

	return props
}

func TestCreateMetadata(t *testing.T) {
	log := logger.NewLogger("test")

	booleanFlagTests := []struct {
		in       string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"false", false},
		{"FALSE", false},
	}

	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataConnectionStringKey], m.connectionString)
		assert.Equal(t, fakeProperties[metadataProtocolKey], m.protocol)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
		assert.Equal(t, fakeProperties[metadataUsernameKey], m.username)
		assert.Equal(t, fakeProperties[metadataPasswordKey], m.password)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		assert.Equal(t, false, m.autoAck)
		assert.Equal(t, false, m.requeueInFailure)
		assert.Equal(t, true, m.deleteWhenUnused)
		assert.Equal(t, false, m.enableDeadLetter)
		assert.Equal(t, false, m.publisherConfirm)
		assert.Equal(t, uint8(0), m.deliveryMode)
		assert.Equal(t, uint8(0), m.prefetchCount)
		assert.Equal(t, int64(0), m.maxLen)
		assert.Equal(t, int64(0), m.maxLenBytes)
		assert.Equal(t, "", m.ClientKey)
		assert.Equal(t, "", m.ClientCert)
		assert.Equal(t, "", m.CACert)
		assert.Equal(t, fanoutExchangeKind, m.exchangeKind)
		assert.Equal(t, classicQueueType, m.queueType)
	})

	invalidDeliveryModes := []string{"3", "10", "-1"}

	for _, deliveryMode := range invalidDeliveryModes {
		t.Run(fmt.Sprintf("deliveryMode value=%s", deliveryMode), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataDeliveryModeKey] = deliveryMode

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.EqualError(t, err, "rabbitmq pub/sub error: invalid RabbitMQ delivery mode, accepted values are between 0 and 2")
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, uint8(0), m.deliveryMode)
		})
	}

	t.Run("deliveryMode is set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataDeliveryModeKey] = "2"

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		assert.Equal(t, uint8(2), m.deliveryMode)
	})

	t.Run("protocol does not match connection string", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataProtocolKey] = "fakeprotocol"

		// act
		_, err := createMetadata(fakeMetaData, log)

		// assert
		if assert.Error(t, err) {
			assert.Equal(t, err.Error(), fmt.Sprintf("%s protocol does not match connection string, protocol: %s, connection string: %s", errorMessagePrefix, fakeMetaData.Properties[metadataProtocolKey], fakeMetaData.Properties[metadataConnectionStringKey]))
		}
	})

	t.Run("connection string is empty, protocol is not empty", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataProtocolKey] = "fakeprotocol"
		fakeMetaData.Properties[metadataConnectionStringKey] = ""

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.Nil(t, err)
		assert.Equal(t, fakeProperties[metadataProtocolKey], m.protocol)
	})

	t.Run("invalid concurrency", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[pubsub.ConcurrencyKey] = "a"

		// act
		_, err := createMetadata(fakeMetaData, log)

		// assert
		assert.Error(t, err)
	})

	t.Run("prefetchCount is set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataPrefetchCountKey] = "1"

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		assert.Equal(t, uint8(1), m.prefetchCount)
	})

	t.Run("tls related properties are set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[pubsub.ClientCert] = "-----BEGIN CERTIFICATE-----\nMIIEZjCCA06gAwIBAgIJAMQ5Az0QUDY+MA0GCSqGSIb3DQEBCwUAMGwxCzAJBgNV\nBAYTAlVTMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2Fs\naXR5MRUwEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRN\nUVRlc3QwHhcNMjIxMTEyMTMyNjEyWhcNMjUwMjE0MTMyNjEyWjBsMQswCQYDVQQG\nEwJVUzETMBEGA1UECAwKRmFrZSBTdGF0ZTEWMBQGA1UEBwwNRmFrZSBMb2NhbGl0\neTEVMBMGA1UECgwMRmFrZSBDb21wYW55MRkwFwYDVQQDDBBkYXByUmFiYml0TVFU\nZXN0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqNwAxLCx5J367N41\net6SLa0aWGLHeU61jTz0VG6tKsLPEw3wD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rb\nq9cwHBPbNxRksFQQZPOBONLkcs1yijjMExyc2GsMLCwUFKZWD73SEZe1Hace9otG\n1FHjQgHs2bYIOckHHGQIampM/5L931A9M6j5JHenF4m7KBIDCCAhvka5fuGFsLIs\nke5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQwQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM\n6uobZ7pWU95S8XEbrRygAKPCXmoVWxfsKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek\n05BgAwIDAQABo4IBCTCCAQUwgYYGA1UdIwR/MH2hcKRuMGwxCzAJBgNVBAYTAlVT\nMRMwEQYDVQQIDApGYWtlIFN0YXRlMRYwFAYDVQQHDA1GYWtlIExvY2FsaXR5MRUw\nEwYDVQQKDAxGYWtlIENvbXBhbnkxGTAXBgNVBAMMEGRhcHJSYWJiaXRNUVRlc3SC\nCQCpmf4h1/pxHTAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAxBgNVHSUEKjAoBggr\nBgEFBQcDAQYIKwYBBQUHAwIGCCsGAQUFBwMDBggrBgEFBQcDBDAvBgNVHREEKDAm\ngglsb2NhbGhvc3SCCTEyNy4wLjAuMYIDOjoxgglsb2NhbC5kZXYwDQYJKoZIhvcN\nAQELBQADggEBAGhAqIEzj5cOr0GRf6uhkx3s2s5WGWJlb+J6J2mam/Zu8Z5olFj+\nOWzCwfw/ZV8q5Domr6ddmgOMz+URrckst86/fh597/uv42KwQt/bBmZCvTrr+QjM\nxDmhCTIF8aRl54DQxIZpPBhvBG1fg9E1NGa426zNuySVz/A10aAPlZ1D94iwHOvR\n9UXDG9JVhYYbrgGKloWog+U8viqzLMFeRyMhp4JL1FbGTq/+2FpYD7nc6xq8nm2G\nvAEJ4Tw1exbJc+fcRXUUrxRXTHxJEThRHycXyMZgIZsIHSYGeQOH6HOwp/t+/IyB\n93KPobjIt25cwepLlRWHsGnjFOu/gulXQ3w=\n-----END CERTIFICATE-----"
		fakeMetaData.Properties[pubsub.CACert] = "-----BEGIN CERTIFICATE-----\nMIIDVDCCAjwCCQCpmf4h1/pxHTANBgkqhkiG9w0BAQsFADBsMQswCQYDVQQGEwJV\nUzETMBEGA1UECAwKRmFrZSBTdGF0ZTEWMBQGA1UEBwwNRmFrZSBMb2NhbGl0eTEV\nMBMGA1UECgwMRmFrZSBDb21wYW55MRkwFwYDVQQDDBBkYXByUmFiYml0TVFUZXN0\nMB4XDTIyMTExMjEzMjU1MVoXDTI1MDIxNDEzMjU1MVowbDELMAkGA1UEBhMCVVMx\nEzARBgNVBAgMCkZha2UgU3RhdGUxFjAUBgNVBAcMDUZha2UgTG9jYWxpdHkxFTAT\nBgNVBAoMDEZha2UgQ29tcGFueTEZMBcGA1UEAwwQZGFwclJhYmJpdE1RVGVzdDCC\nASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL1rXcV0uN2UGaSizscS8Sav\n5ifZRXTGLx8vUpt7fDGgO9zZpZrTluduK7ReZrHN/5cML0TQuHR6CE5bi4L4PLc+\nH4Ir2i2aDXQ7kTfxFEK/M9q57nKTepu7Wu1u0MDdpLzB83huTDPX0AksDR+8e4cT\nLmxMJ0EkifEJrBdffLPoYKsdG9Fdrk3KS6NWFEIIAamCNRhrMX1DsEd2yOsOzbhK\nX1m5/g9jyDdaZYUOb2j7li8b0D+PZMKNukaEwZt7OAy8vkgOXup/H5Jq4RKUxiOi\nPczk0xVAl5i+cLpcAHBl8nL/ryMosHQZujEqIU5buy6aRDHY8PmZbvmYqbhFjEkC\nAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAsebNZV6B4OoO5Tf/NiGHotYrny69FoHH\ncSULpGShl3PFJnLzeMaq80R2v/pQ3EH1SjO0k0dcprNsJ35JJwBjLrV1n7ZsMVZp\n2i/2WkPiOuGncnwq4LioAl2rm2GtgBTua0WHey1dAUUtg7qG2yxsXCzBVXL/rDTv\nzdADW+IiFW81FusIs3WbeMXZxNyWZD9tfLsqjSxVqBn6ER4+9rrWCxEOPoApE8IY\nAp6GgG3wlCr2IheBgL4QI1FaYl/ZAXAlzh0IS1X1HUjX+pKJ0nboNF8H1XTT5FSN\n8FsHHG+vEsXg6/In7v/F1akOSF63PDhjAL696ouwnbOj5jRUx3zYUQ==\n-----END CERTIFICATE-----\n"
		fakeMetaData.Properties[pubsub.ClientKey] = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpQIBAAKCAQEAqNwAxLCx5J367N41et6SLa0aWGLHeU61jTz0VG6tKsLPEw3w\nD7jSOefV4Wxows+ZA54XoyNTOzk6W0Rbq9cwHBPbNxRksFQQZPOBONLkcs1yijjM\nExyc2GsMLCwUFKZWD73SEZe1Hace9otG1FHjQgHs2bYIOckHHGQIampM/5L931A9\nM6j5JHenF4m7KBIDCCAhvka5fuGFsLIske5e9LlraPHZgM3EX2qrkOsLZ0Ll7JQw\nQE1/Kg5Tbk/DdQLjYAr+I1VEmHLpDPrM6uobZ7pWU95S8XEbrRygAKPCXmoVWxfs\nKWTVy6vuRtq8iGsIvWNWssOKB2V4U7Ek05BgAwIDAQABAoIBAQCHYTNgfoVxVFN8\nGr9nzhAGfNU1baSdlkQqg2ckVkORl/QrQ65m/HfXow6wF5l6LrRv2Qz8Z3LwdXZa\n+9g/Ulwo9qZ3Z2s+j3dBaJT+uN4dSKf/N4AuBm+dd12gAIrs71rqbfaA0k+MAZPq\neEmGKkz4e9Cnz7CSm6CO24h/wNAQyjwi+/QBxKVix5+BKgMK2AQd/xmlzbGxGO6T\n0UTRCbx6CaitX7I6sLU8C9ebcsB2lO/y+VDPeVU/ij0zLQJLCY0px/TmRrcak+WD\n/POnL3JSr6AqvGoOhYkSTkzijEjHMZwZM4pakoI5vSGWDIxmb8GpnmwTjRy5R1e6\nTShbO4bxAoGBANZlfLZVsLGHN3o4OShVjI1qJomHIEFsbeT39KtCNIwwDZuoOWB7\nH4ENwbLBH4kAWOqvdoqxhsic0RHFsSaxn4ubA9q1XA1oLzu2HlAiBG4hSUhclXI8\nzWREXYmyCgKXrje9gMn3g+cfTFIFdJSLcQgaBRVFNuuj/z8BTHCIK1gLAoGBAMmg\nYmnzsRncuwREuOGmfu+NtyO3e3tMacce5DxsAZzYBdnmxUnfPUbqdivSfM8ls9gD\nXoQnqkfA7lK/kk2KgYFzHulSlXhVUoMzQMJR86DcMtyUXw6Y4TShXoxmcH6fd4bb\ndFanPGnqF7+rrPnvrLlbJavce8Yv99HghVhFeHrpAoGBAJLvHOiNk7ondIMD01Bs\nSRaGAIFSpW2RFUPOF9XjWsYVDM54HVKdLzswJWcP6j/niAjXVgO5pSkKKFLozP86\nzqBMDfkvQDZEB9MBfobmuWiM0X+RTC7tssob/IspBKlAMPssmf5Q7wpQLessp/hC\nFKY7xu4L8JbQX1db2XpqKGJ/AoGBAKCFX9SaftToNrwfOlHsDAMMAFIfMd/n5g7x\nOSQhmOcV1RK19zvozxA2ef5JhbQlgBjqFcWBDsAxiYu3mPLC4nf8/w8jAHzc8OQj\nWdtbA2t948BZH5Svghw0nOGKbOVib/adoPGcEyz/ggjG1N/nQMwRFNzOnHwclGAz\nL/Ym2NSBAoGAOQW6/CQhLCS3qyh89NzQSL8FUEYskOSthxqnRJyCDpNtVR4JGoG7\nSqjY4tMCm30X4qJuBUuhymtdCDRMKfcBEQ1RcfQgW/DZEsOk5OqobF84ve8eJ89F\nWZuSgACcvoYumRQ8+fp4nQ74EQloOkKmvWTUbguCZLEnecpYkzw/RhU=\n-----END RSA PRIVATE KEY-----\n"

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.NotNil(t, m.TLSProperties.ClientKey, "failed to parse valid client certificate key")
		block, _ := pem.Decode([]byte(m.TLSProperties.ClientCert))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse client certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprRabbitMQTest", cert.Subject.CommonName)

		block, _ = pem.Decode([]byte(m.TLSProperties.CACert))
		cert, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("failed to parse ca certificate from metadata. %v", err)
		}
		assert.Equal(t, "daprRabbitMQTest", cert.Subject.CommonName)
	})

	t.Run("maxLen and maxLenBytes is set", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataMaxLenKey] = "1"
		fakeMetaData.Properties[metadataMaxLenBytesKey] = "2000000"

		// act
		m, err := createMetadata(fakeMetaData, log)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
		assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
		assert.Equal(t, int64(1), m.maxLen)
		assert.Equal(t, int64(2000000), m.maxLenBytes)
	})

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("autoAck value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataAutoAckKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.autoAck)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("requeueInFailure value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataRequeueInFailureKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.requeueInFailure)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("deleteWhenUnused value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataDeleteWhenUnusedKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.deleteWhenUnused)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("durable value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataDurableKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.durable)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("publisherConfirm value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataPublisherConfirmKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.publisherConfirm)
		})
	}

	for _, tt := range booleanFlagTests {
		t.Run(fmt.Sprintf("enableDeadLetter value=%s", tt.in), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataEnableDeadLetterKey] = tt.in

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, tt.expected, m.enableDeadLetter)
		})
	}
	validExchangeKind := []string{amqp.ExchangeDirect, amqp.ExchangeTopic, amqp.ExchangeFanout, amqp.ExchangeHeaders}

	for _, exchangeKind := range validExchangeKind {
		t.Run(fmt.Sprintf("exchangeKind value=%s", exchangeKind), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataExchangeKindKey] = exchangeKind

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, exchangeKind, m.exchangeKind)
		})
	}

	t.Run("exchangeKind is invalid", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataExchangeKindKey] = "invalid"

		// act
		_, err := createMetadata(fakeMetaData, log)

		// assert
		assert.Error(t, err)
	})

	validQueueType := []string{amqp.QueueTypeClassic, amqp.QueueTypeQuorum, amqp.QueueTypeStream}

	for _, queueType := range validQueueType {
		t.Run(fmt.Sprintf("queueType value=%s", queueType), func(t *testing.T) {
			fakeProperties := getFakeProperties()

			fakeMetaData := pubsub.Metadata{
				Base: mdata.Base{Properties: fakeProperties},
			}
			fakeMetaData.Properties[metadataQueueType] = queueType

			// act
			m, err := createMetadata(fakeMetaData, log)

			// assert
			assert.NoError(t, err)
			assert.Equal(t, fakeProperties[metadataHostnameKey], m.hostname)
			assert.Equal(t, fakeProperties[metadataConsumerIDKey], m.consumerID)
			assert.Equal(t, queueType, m.queueType)
		})
	}

	t.Run("queueType is invalid", func(t *testing.T) {
		fakeProperties := getFakeProperties()

		fakeMetaData := pubsub.Metadata{
			Base: mdata.Base{Properties: fakeProperties},
		}
		fakeMetaData.Properties[metadataQueueType] = "invalid"

		// act
		_, err := createMetadata(fakeMetaData, log)

		// assert
		assert.Error(t, err)
	})
}

func TestConnectionURI(t *testing.T) {
	log := logger.NewLogger("test")

	testCases := []struct {
		args           map[string]string
		expectedOutput string
	}{
		// connection string
		{
			args:           map[string]string{"connectionString": "amqp://fakeuser:fakepassword@fakehostname-connectionstring"},
			expectedOutput: "amqp://fakeuser:fakepassword@fakehostname-connectionstring",
		},

		// individual arguments
		{
			args:           map[string]string{},
			expectedOutput: "amqp://localhost",
		},
		{
			args:           map[string]string{"hostname": "localhost"},
			expectedOutput: "amqp://localhost",
		},
		{
			args:           map[string]string{"hostname": "fake-hostname", "password": "testpassword"},
			expectedOutput: "amqp://fake-hostname",
		},
		{
			args:           map[string]string{"hostname": "localhost", "username": "testusername"},
			expectedOutput: "amqp://testusername@localhost",
		},
		{
			args:           map[string]string{"hostname": "localhost", "username": "testusername", "password": "testpassword"},
			expectedOutput: "amqp://testusername:testpassword@localhost",
		},
		{
			args:           map[string]string{"protocol": "amqps", "hostname": "localhost", "username": "testusername", "password": "testpassword"},
			expectedOutput: "amqps://testusername:testpassword@localhost",
		},

		// legacy argument
		{
			args:           map[string]string{"host": "amqp://fake-hostname"},
			expectedOutput: "amqp://fake-hostname",
		},
	}

	var metadata pubsub.Metadata

	for _, testCase := range testCases {
		metadata = pubsub.Metadata{
			Base: mdata.Base{Properties: testCase.args},
		}

		m, err := createMetadata(metadata, log)

		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedOutput, m.connectionURI())
	}
}
