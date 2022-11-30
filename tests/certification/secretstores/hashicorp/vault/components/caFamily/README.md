# Certificate-related tests

These tests are particularly tricky to setup- so a little README in case you decide to change
this in the future and things break.

# vault, `-dev-tls` and its default port

To make our testing easier we start vault with `-dev-tls`. We do this:
* to keep the development behavior on,
* to force vault to start with a default TLS listener with its own self-signed  TLS cert. We will
  use this listener for our negative tests (for `skipValidation` and `tlsServerName`).

To keep the rest of the test setup consistent and similar to other tests, we move this listener to a non-default port.


# Using and generating our very own certificate and key

Besides `-dev-tls`, we also instruct vault to use a configuration that defines another listener using `-config /vault/config/vault_server.hcl`. This listener, defined in the `config/vault_server.hcl` is configured use a certificate-key pair we generated ourselves. It also binds this listener to the default vault port - to make the keep some sort of consistency in the test setup.

We use this certificate we generated to assist with the validation of `caPem`, `caCert`, `caPath`, `skipValidate` and `tlsServerName` flags. All of these refer to the same certificate. Testing `caPem` is a bit special in that it needs the certificate inlined in the component YAML file.

A Makefile is included here in order to document and to ease re-generation of the certificate and keys. It will also re-generate the `caPem`-dependent component YAML, so one does not have to remember updating it whenever the certificate is updated or regenerated.
As a matter of fact, our code does not ship with any of these certificates. Instead, this Makefile is invoked at the begging on `TestCaFamilyOfFields` test.

# Misc. references

For how to configure the vault docker image we are using check https://hub.docker.com/_/vault/