module github.com/dapr/components-contrib

go 1.16

require (
	cloud.google.com/go v0.65.0
	cloud.google.com/go/datastore v1.1.0
	cloud.google.com/go/pubsub v1.3.1
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-event-hubs-go v1.3.1
	github.com/Azure/azure-sdk-for-go v48.2.0+incompatible
	github.com/Azure/azure-service-bus-go v0.10.10
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.13.1
	github.com/Azure/go-autorest/autorest v0.11.12
	github.com/Azure/go-autorest/autorest/adal v0.9.5
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.23.1
	github.com/a8m/documentdb v1.2.1-0.20190920062420-efdd52fe0905
	github.com/aerospike/aerospike-client-go v2.7.0+incompatible
	github.com/agrea/ptr v0.0.0-20180711073057-77a518d99b7b
	github.com/ajg/form v1.5.1 // indirect
	github.com/alicebob/miniredis/v2 v2.13.3
	github.com/aliyun/aliyun-oss-go-sdk v2.0.7+incompatible
	github.com/apache/pulsar-client-go v0.1.0
	github.com/apache/thrift v0.14.0 // indirect
	github.com/aws/aws-sdk-go v1.27.0
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cenkalti/backoff/v4 v4.1.0
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/cyphar/filepath-securejoin v0.2.2
	github.com/dancannon/gorethink v4.0.0+incompatible
	github.com/dapr/kit v0.0.1
	github.com/denisenkom/go-mssqldb v0.0.0-20191128021309-1d7a30a10f73
	github.com/dghubble/go-twitter v0.0.0-20190719072343-39e5462e111f
	github.com/dghubble/oauth1 v0.6.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/didip/tollbooth v4.0.2+incompatible
	github.com/dnaeon/go-vcr v1.1.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.3.2
	github.com/fasthttp-contrib/sessions v0.0.0-20160905201309-74f6ac73d5d5
	github.com/fatih/structs v1.1.0 // indirect
	github.com/gavv/httpexpect v2.0.0+incompatible // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/go-redis/redis/v7 v7.0.1
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20191018090344-07ace3bab0f8
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.7.3
	github.com/grandcat/zeroconf v0.0.0-20190424104450-85eadb44205c
	github.com/hashicorp/consul/api v1.3.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hazelcast/hazelcast-go-client v0.0.0-20190530123621-6cf767c2f31a
	github.com/imkira/go-interpol v1.1.0 // indirect
	github.com/influxdata/influxdb-client-go v1.4.0
	github.com/jackc/pgx/v4 v4.6.0
	github.com/json-iterator/go v1.1.10
	github.com/kataras/go-errors v0.0.3 // indirect
	github.com/kataras/go-serializer v0.0.4 // indirect
	github.com/keighl/postmark v0.0.0-20190821160221-28358b1a94e3
	github.com/microcosm-cc/bluemonday v1.0.7 // indirect
	github.com/mitchellh/mapstructure v1.3.3
	github.com/nacos-group/nacos-sdk-go v1.0.7
	github.com/moul/http2curl v1.0.0 // indirect
	github.com/nats-io/nats-server/v2 v2.2.1 // indirect
	github.com/nats-io/nats-streaming-server v0.21.1 // indirect
  github.com/nats-io/go-nats v1.7.2
	github.com/nats-io/nats.go v1.10.1-0.20210330225420-a0b1f60162f8
	github.com/nats-io/stan.go v0.8.3
	github.com/open-policy-agent/opa v0.23.2
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da
	github.com/sendgrid/rest v2.6.3+incompatible // indirect
	github.com/sendgrid/sendgrid-go v3.5.0+incompatible
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/pretty v1.1.0 // indirect
	github.com/valyala/fasthttp v1.19.0
	github.com/vmware/vmware-go-kcl v0.0.0-20191104173950-b6c74c3fe74e
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/yalp/jsonpath v0.0.0-20180802001716-5cc68e5049a0 // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	go.mongodb.org/mongo-driver v1.1.2
	goji.io v2.0.2+incompatible // indirect
	golang.org/x/crypto v0.0.0-20210314154223-e6e6c4f2bb5b
	golang.org/x/net v0.0.0-20210331212208-0fccb6fa2b5c
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43
	google.golang.org/api v0.32.0
	google.golang.org/genproto v0.0.0-20201204160425-06b3db808446
	google.golang.org/grpc v1.34.0
	gopkg.in/alexcesaro/quotedprintable.v3 v3.0.0-20150716171945-2caba252f4dc // indirect
	gopkg.in/couchbase/gocb.v1 v1.6.4
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.0 // indirect
	gopkg.in/gomail.v2 v2.0.0-20160411212932-81ebce5c23df
	gopkg.in/gorethink/gorethink.v4 v4.1.0 // indirect
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/kataras/go-serializer.v0 v0.0.4 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c
	k8s.io/api v0.20.0
	k8s.io/apiextensions-apiserver v0.20.0
	k8s.io/apimachinery v0.20.0
	k8s.io/client-go v0.20.0
)

replace k8s.io/client => github.com/kubernetes-client/go v0.0.0-20190928040339-c757968c4c36
