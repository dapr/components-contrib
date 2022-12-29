listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = "false"
  tls_cert_file = "/certificates/cert.pem"
  tls_key_file  = "/certificates/key.pem"
}