module github.com/dapr/components-contrib/tests/e2e/pubsub/jetstream

go 1.18

require (
	github.com/dapr/components-contrib v1.5.1
	github.com/dapr/kit v0.0.2-0.20210614175626-b9074b64d233
)

require (
	github.com/cenkalti/backoff/v4 v4.1.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/nats-io/nats.go v1.13.1-0.20220308171302-2f2f6968e98d // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	golang.org/x/crypto v0.0.0-20220131195533-30dcbda58838 // indirect
	golang.org/x/sys v0.0.0-20220412211240-33da011f77ad // indirect
)

replace github.com/dapr/components-contrib => ../../../../
