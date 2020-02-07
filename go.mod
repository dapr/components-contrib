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
	github.com/Azure/azure-sdk-for-go v33.4.0+incompatible
	github.com/Azure/azure-service-bus-go v0.9.1
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-autorest/autorest v0.9.2
	github.com/Azure/go-autorest/autorest/adal v0.8.0
	github.com/Azure/go-autorest/autorest/azure/auth v0.3.0
	github.com/Azure/go-autorest/autorest/to v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.2.0 // indirect
	github.com/Shopify/sarama v1.23.1
	github.com/a8m/documentdb v1.2.0
	github.com/aerospike/aerospike-client-go v2.7.0+incompatible
	github.com/ajg/form v1.5.1 // indirect
	github.com/apache/thrift v0.13.0 // indirect
	github.com/aws/aws-sdk-go v1.25.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.17+incompatible // indirect
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/denisenkom/go-mssqldb v0.0.0-20191128021309-1d7a30a10f73
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/didip/tollbooth v4.0.2+incompatible
	github.com/dnaeon/go-vcr v1.0.1 // indirect
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/fasthttp-contrib/sessions v0.0.0-20160905201309-74f6ac73d5d5
	github.com/fasthttp-contrib/websocket v0.0.0-20160511215533-1f3b11f56072 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/garyburd/redigo v1.6.0 // indirect
	github.com/gavv/httpexpect v2.0.0+incompatible // indirect
	github.com/go-redis/redis v6.15.5+incompatible
	github.com/gocql/gocql v0.0.0-20191018090344-07ace3bab0f8
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grandcat/zeroconf v0.0.0-20190424104450-85eadb44205c
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/hashicorp/consul/api v1.2.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/hazelcast/hazelcast-go-client v0.0.0-20190530123621-6cf767c2f31a
	github.com/imkira/go-interpol v1.1.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/joomcode/errorx v1.0.0 // indirect
	github.com/joomcode/redispipe v0.9.0
	github.com/json-iterator/go v1.1.7
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/kataras/go-errors v0.0.3 // indirect
	github.com/kataras/go-serializer v0.0.4 // indirect
	github.com/kubernetes-client/go v0.0.0-20190625181339-cd8e39e789c7
	github.com/mediocregopher/radix.v2 v0.0.0-20181115013041-b67df6e626f9 // indirect
	github.com/microcosm-cc/bluemonday v1.0.2 // indirect
	github.com/moul/http2curl v1.0.0 // indirect
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/go-nats v1.7.2
	github.com/nats-io/nats-server/v2 v2.1.2 // indirect
	github.com/nats-io/nats-streaming-server v0.16.2 // indirect
	github.com/nats-io/nats.go v1.9.1
	github.com/nats-io/stan.go v0.6.0
	github.com/onsi/ginkgo v1.10.2 // indirect
	github.com/onsi/gomega v1.7.0 // indirect
	github.com/openzipkin/zipkin-go v0.1.6
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/pkg/errors v0.8.1
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/russross/blackfriday v2.0.0+incompatible // indirect
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da
	github.com/satori/go.uuid v1.2.0
	github.com/sergi/go-diff v1.0.0 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/stretchr/testify v1.4.0
	github.com/tidwall/pretty v1.0.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/valyala/fasthttp v1.6.0
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/yalp/jsonpath v0.0.0-20180802001716-5cc68e5049a0 // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yudai/pp v2.0.1+incompatible // indirect
	github.com/yuin/gopher-lua v0.0.0-20191220021717-ab39c6098bdb // indirect
	go.etcd.io/etcd v3.3.17+incompatible
	go.mongodb.org/mongo-driver v1.1.2
	go.opencensus.io v0.22.2
	go.uber.org/multierr v1.2.0 // indirect
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	google.golang.org/api v0.15.0
	google.golang.org/genproto v0.0.0-20200122232147-0452cf42e150
	google.golang.org/grpc v1.26.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/couchbase/gocb.v1 v1.6.4
	gopkg.in/couchbase/gocbcore.v7 v7.1.15 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.3 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.0 // indirect
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
	gopkg.in/kataras/go-serializer.v0 v0.0.4 // indirect
	gopkg.in/square/go-jose.v2 v2.4.1 // indirect
	gopkg.in/yaml.v2 v2.2.7 // indirect
	k8s.io/apimachinery v0.0.0-20190817020851-f2f3a405f61d
	k8s.io/client v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.3.0+incompatible
	k8s.io/client => github.com/kubernetes-client/go v0.0.0-20190928040339-c757968c4c36
)
