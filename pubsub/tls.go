package pubsub

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

type TLSProperties struct {
	CACert     string
	ClientCert string
	ClientKey  string
}

const (
	CACert     = "caCert"
	ClientCert = "clientCert"
	ClientKey  = "clientKey"
)

// TLS takes a metadata object and returns the TLSProperties configured.
func TLS(metadata map[string]string) (TLSProperties, error) {
	cfg := TLSProperties{}

	if val, ok := metadata[CACert]; ok && val != "" {
		if !isValidPEM(val) {
			return TLSProperties{}, errors.New("invalid caCert")
		}
		cfg.CACert = val
	}
	if val, ok := metadata[ClientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return TLSProperties{}, errors.New("invalid clientCert")
		}
		cfg.ClientCert = val
	}
	if val, ok := metadata[ClientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return TLSProperties{}, errors.New("invalid clientKey")
		}
		cfg.ClientKey = val
	}

	return cfg, nil
}

func ConvertTLSPropertiesToTLSConfig(properties TLSProperties) (*tls.Config, error) {
	tlsConfig := new(tls.Config)

	if properties.ClientCert != "" && properties.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(properties.ClientCert), []byte(properties.ClientKey))
		if err != nil {
			return &tls.Config{}, fmt.Errorf("unable to load client certificate and key pair. Err: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if properties.CACert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(properties.CACert)); !ok {
			return &tls.Config{}, fmt.Errorf("unable to load CA certificate")
		}
	}

	return tlsConfig, nil
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}
