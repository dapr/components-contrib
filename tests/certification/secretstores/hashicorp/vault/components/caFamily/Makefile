
TARGETS = certificates/key.pem certificates/cert.pem caPem/hashicorp-vault.yml certificates/incorrect-ca-key.pem certificates/incorrect-ca-cert.pem

all: $(TARGETS)

# Create cert and key lasting 10 years, no password,  no prompt for
# subject details. Also set subjectAltName so we avoid the
# "x509: certificate relies on legacy Common Name field" errors

certificates/key.pem certificates/cert.pem:
	set -e; \
	mkdir -v -p certificates; \
	openssl req -x509 -newkey rsa:4096 \
		-keyout certificates/key.pem \
		-out certificates/cert.pem \
		-sha256 -days 3650 \
		-nodes \
		-addext "subjectAltName = DNS:hashicorp_vault,DNS:localhost,IP:127.0.0.1" \
		-subj "/C=CA/ST=BC/L=Vancouver/O=Dapr Testing/OU=Org/CN=www.dapr.io"; \
	chmod -v 644 certificates/key.pem certificates/cert.pem

# We use this for negative tests that ensure we reject connecting to
# a server using a distinct Certificate Authority -- despite the server certificate
# having all the right identifiers
certificates/incorrect-ca-key.pem certificates/incorrect-ca-cert.pem:
	set -e; \
	mkdir -v -p certificates; \
	openssl req -x509 -newkey rsa:4096 \
		-keyout certificates/incorrect-ca-key.pem \
		-out certificates/incorrect-ca-cert.pem \
		-sha256 -days 3650 \
		-nodes \
		-addext "subjectAltName = DNS:hashicorp_vault,DNS:localhost,IP:127.0.0.1" \
		-subj "/C=CA/ST=BC/L=Vancouver/O=Dapr Testing/OU=Org/CN=www.dapr.io" ; \
	chmod -v 644 certificates/incorrect-ca-key.pem certificates/incorrect-ca-cert.pem
		

caPem/hashicorp-vault.yml: caPem/hashicorp-vault.yml.template certificates/cert.pem
	set -e; \
	echo "#\n# THIS FILE IS AUTO-GENERATED - DO NOT EDIT\n#\n\n" > $@.tmp; \
	cat caPem/hashicorp-vault.yml.template >>  $@.tmp;\
	sed 's/^/          /' certificates/cert.pem >> $@.tmp; \
	mv -f $@.tmp $@

# %: .tmp.%
# 	mv $< $@

clean:
	rm -f -v $(TARGETS)
	rmdir certificates

.PHONY: clean
.PHONY: all