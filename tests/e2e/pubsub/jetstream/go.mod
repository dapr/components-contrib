module github.com/dapr/components-contrib/tests/e2e/pubsub/jetstream

go 1.22.0

toolchain go1.22.2

require (
	github.com/dapr/components-contrib v1.10.6-0.20230403162214-9ee9d56cb7ea
	github.com/dapr/kit v0.13.1-0.20240306152601-e33fbab74548
)

require (
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cloudevents/sdk-go/binding/format/protobuf/v2 v2.14.0 // indirect
	github.com/cloudevents/sdk-go/v2 v2.14.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.7 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nats-io/nats.go v1.28.0 // indirect
	github.com/nats-io/nkeys v0.4.6 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/cast v1.5.1 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	google.golang.org/protobuf v1.32.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/apimachinery v0.26.10 // indirect
)

replace github.com/dapr/components-contrib => ../../../../
