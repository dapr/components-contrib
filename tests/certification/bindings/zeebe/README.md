# Run the tests on the command line

## Command bindings

```bash
cd tests/certification/bindings/zeebe/command
GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore go test -v --count=1 .
```

or a single command binding

```bash
cd tests/certification/bindings/zeebe/command
GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore go test -v --count=1 ./deploy_resource_test.go ./command_test.go
```

## Jobworker bindings

```bash
cd tests/certification/bindings/zeebe/jobworker
GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore go test -v --count=1 .
```

# TLS

Zeebe supports TLS connection between the client and Zeebe itself: https://docs.camunda.io/docs/self-managed/zeebe-deployment/security/secure-client-communication/

The tests will test TLS and non-TLS connections. Therefore a cert and a private key needs to be created.

## Generate cert

For the test there is a predefined cert and key with a validity of 10 years. The cert was generated with the following command:

```bash
cd tests/certification/bindings/zeebe/certs
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 3650 --nodes --addext 'subjectAltName=IP:127.0.0.1, DNS:localhost'
```

### Automatic cert generation

There was also an attempt to generate the cert for each test automatically. The problem is that when using the automatic generated cert, the connection could not be established because of the following error: 

```
transport: authentication handshake failed: EOF
```

The code that was used:

```go
func CreateKeyAndCert(ctx flow.Context) error {
	keyFile, err := os.Create(TlsKeyFile)
	if err != nil {
		return err
	}
	defer keyFile.Close()

	certFile, err := os.Create(TlsCertFile)
	if err != nil {
		return err
	}
	defer certFile.Close()

	// This helper function with modifications comes from https://github.com/madflojo/testcerts/blob/main/testcerts.go
	// Copyright 2019 Benjamin Cane, MIT License

	// Create a Certificate Authority Cert
	ca := &x509.Certificate{
		Subject: pkix.Name{
			Organization: []string{"Dapr Development Only Organization"},
			CommonName:   "localhost",
		},
		SerialNumber:          big.NewInt(123),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:              []string{"localhost"},
	}

	// Create a Private and Public Key
	keypair, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("could not generate rsa key - %s", err)
	}

	// Use CA Cert to sign a CSR and create a Public Certificate
	cert, err := x509.CreateCertificate(rand.Reader, ca, ca, &keypair.PublicKey, keypair)
	if err != nil {
		return fmt.Errorf("could not generate certificate - %s", err)
	}

	// Convert keys into pem.Block
	publiccert := &pem.Block{Type: "CERTIFICATE", Bytes: cert}
	privatekey := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(keypair)}
	err = pem.Encode(certFile, publiccert)
	if err != nil {
		return err
	}
	err = pem.Encode(keyFile, privatekey)
	if err != nil {
		return err
	}

	return nil
}
```

# BPMN process files

The test suite uses two BPMN processes which are located in the `processes` folder. These processes can be edited with the [Camunda Modeler](https://camunda.com/de/products/camunda-platform/modeler/)

# Missing tests

## Incident related tests

Currently it's not possible to get an incident key which is needed to resolve an incident.
