module github.com/dapr/components-contrib/tests/e2e/pubsub/jetstream

go 1.19

require (
	github.com/dapr/components-contrib v1.5.1
	github.com/dapr/kit v0.0.3
)

require (
	github.com/cenkalti/backoff/v4 v4.2.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/nats-io/nats.go v1.19.1 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
)

replace github.com/dapr/components-contrib => ../../../../

replace github.com/dapr/dapr => github.com/mukundansundar/dapr v0.7.1-0.20221214060805-f7f65f61c0bb
