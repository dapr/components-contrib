listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = "false"
  tls_cert_file = "/certificates/cert.pem"
  tls_key_file  = "/certificates/key.pem"
}

api_addr = "https://127.0.0.1:8201"

#backend "file" {
#  path = "/vault/file"
#}

#default_lease_ttl = "168h"
#max_lease_ttl = "720h"
