module github.com/dapr/components-contrib

go 1.13

require (
	cloud.google.com/go v0.52.0
	cloud.google.com/go/datastore v1.0.0
	cloud.google.com/go/pubsub v1.0.1
	cloud.google.com/go/storage v1.0.0
	contrib.go.opencensus.io/exporter/ocagent v0.6.0
	contrib.go.opencensus.io/exporter/zipkin v0.1.1
	github.com/Azure/azure-event-hubs-go v1.3.1
	github.com/Azure/azure-sdk-for-go v37.1.0+incompatible
	github.com/Azure/azure-service-bus-go v0.9.1
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-autorest/autorest v0.9.3
	github.com/Azure/go-autorest/autorest/adal v0.8.1
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2
	github.com/Shopify/sarama v1.23.1
	github.com/a8m/documentdb v1.2.0
	github.com/aerospike/aerospike-client-go v2.7.0+incompatible
	github.com/aws/aws-sdk-go v1.25.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/dapr/dapr v0.4.1-0.20200228055659-71892bc0111e
	github.com/denisenkom/go-mssqldb v0.0.0-20191128021309-1d7a30a10f73
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/didip/tollbooth v4.0.2+incompatible
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/fasthttp-contrib/sessions v0.0.0-20160905201309-74f6ac73d5d5
	github.com/go-redis/redis/v7 v7.0.1
	github.com/gocql/gocql v0.0.0-20191018090344-07ace3bab0f8
	github.com/golang/mock v1.4.0
	github.com/golang/protobuf v1.3.3
	github.com/google/uuid v1.1.1
	github.com/grandcat/zeroconf v0.0.0-20190424104450-85eadb44205c
	github.com/hashicorp/consul/api v1.2.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/hazelcast/hazelcast-go-client v0.0.0-20190530123621-6cf767c2f31a
	github.com/joomcode/errorx v1.0.1 // indirect
	github.com/joomcode/redispipe v0.9.0
	github.com/json-iterator/go v1.1.8
	github.com/kubernetes-client/go v0.0.0-20190625181339-cd8e39e789c7
	github.com/nats-io/gnatsd v1.4.1
	github.com/nats-io/go-nats v1.7.2
	github.com/nats-io/nats-streaming-server v0.17.0 // indirect
	github.com/nats-io/nats.go v1.9.1
	github.com/nats-io/stan.go v0.6.0
	github.com/openzipkin/zipkin-go v0.1.6
	github.com/pkg/errors v0.8.1
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da
	github.com/satori/go.uuid v1.2.0
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/stretchr/testify v1.4.0
	github.com/tidwall/pretty v1.0.1 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20200122045848-3419fae592fc // indirect
	github.com/valyala/fasthttp v1.6.0
	go.etcd.io/etcd v3.3.17+incompatible
	go.mongodb.org/mongo-driver v1.1.2
	go.opencensus.io v0.22.3
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	google.golang.org/api v0.15.0
	google.golang.org/genproto v0.0.0-20200122232147-0452cf42e150
	google.golang.org/grpc v1.26.0
	gopkg.in/couchbase/gocb.v1 v1.6.4
	gopkg.in/couchbase/gocbcore.v7 v7.1.16 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	k8s.io/apimachinery v0.17.0
	k8s.io/client-go v0.17.0
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.3.0+incompatible
	k8s.io/client => github.com/kubernetes-client/go v0.0.0-20190928040339-c757968c4c36
)
