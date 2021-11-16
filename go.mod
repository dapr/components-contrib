module github.com/dapr/components-contrib

go 1.16

require (
	cloud.google.com/go v0.65.0
	cloud.google.com/go/datastore v1.1.0
	cloud.google.com/go/pubsub v1.5.0
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-amqp-common-go/v3 v3.1.0
	github.com/Azure/azure-event-hubs-go/v3 v3.3.10
	github.com/Azure/azure-sdk-for-go v57.2.0+incompatible
	github.com/Azure/azure-sdk-for-go/sdk/azcore v0.20.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v0.12.0
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets v0.3.0
	github.com/Azure/azure-service-bus-go v0.10.10
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.13.1
	github.com/Azure/go-autorest/autorest v0.11.21
	github.com/Azure/go-autorest/autorest/adal v0.9.16
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.8
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.23.1
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/a8m/documentdb v1.3.1-0.20211026005403-13c3593b3c3a
	github.com/aerospike/aerospike-client-go v4.5.0+incompatible
	github.com/agrea/ptr v0.0.0-20180711073057-77a518d99b7b
	github.com/ajg/form v1.5.1 // indirect
	github.com/alibaba/sentinel-golang v1.0.3
	github.com/alicebob/miniredis/v2 v2.13.3
	github.com/aliyun/aliyun-oss-go-sdk v2.0.7+incompatible
	github.com/aliyun/aliyun-tablestore-go-sdk v1.6.0
	github.com/andybalholm/brotli v1.0.1 // indirect
	github.com/apache/pulsar-client-go v0.6.1-0.20211027182823-171ef578e91a
	github.com/apache/rocketmq-client-go/v2 v2.1.0
	github.com/apache/thrift v0.14.0 // indirect
	github.com/asaskevich/EventBus v0.0.0-20200907212545-49d423059eef
	github.com/aws/aws-sdk-go v1.36.30
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/camunda-cloud/zeebe/clients/go v1.0.1
	github.com/cenkalti/backoff/v4 v4.1.1
	github.com/cinience/go_rocketmq v0.0.2
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/cyphar/filepath-securejoin v0.2.2
	github.com/dancannon/gorethink v4.0.0+incompatible
	github.com/dapr/kit v0.0.2-0.20210614175626-b9074b64d233
	github.com/deepmap/oapi-codegen v1.8.1 // indirect
	github.com/denisenkom/go-mssqldb v0.0.0-20210411162248-d9abbec934ba
	github.com/dghubble/go-twitter v0.0.0-20190719072343-39e5462e111f
	github.com/dghubble/oauth1 v0.6.0
	github.com/didip/tollbooth v4.0.2+incompatible
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.3.5
	github.com/fasthttp-contrib/sessions v0.0.0-20160905201309-74f6ac73d5d5
	github.com/fatih/color v1.10.0 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/gavv/httpexpect v2.0.0+incompatible // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/go-errors/errors v1.4.0 // indirect
	github.com/go-logr/logr v0.3.0 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/go-redis/redis/v8 v8.8.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20210515062232-b7ef815b4556
	github.com/gofrs/flock v0.8.1
	github.com/golang-jwt/jwt v3.2.1+incompatible
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.4.3
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/uuid v1.2.0
	github.com/googleapis/gnostic v0.5.1 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grandcat/zeroconf v0.0.0-20190424104450-85eadb44205c
	github.com/hashicorp/consul/api v1.3.0
	github.com/hashicorp/go-multierror v1.0.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hazelcast/hazelcast-go-client v0.0.0-20190530123621-6cf767c2f31a
	github.com/imdario/mergo v0.3.10 // indirect
	github.com/imkira/go-interpol v1.1.0 // indirect
	github.com/influxdata/influxdb-client-go v1.4.0
	github.com/influxdata/line-protocol v0.0.0-20210311194329-9aa0e372d097 // indirect
	github.com/jackc/pgx/v4 v4.6.0
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.2.0 // indirect
	github.com/json-iterator/go v1.1.11
	github.com/kataras/go-errors v0.0.3 // indirect
	github.com/kataras/go-serializer v0.0.4 // indirect
	github.com/keighl/postmark v0.0.0-20190821160221-28358b1a94e3
	github.com/kr/text v0.2.0 // indirect
	github.com/machinebox/graphql v0.2.2
	github.com/matryer/is v1.4.0 // indirect
	github.com/mattn/go-isatty v0.0.13 // indirect
	github.com/microcosm-cc/bluemonday v1.0.7 // indirect
	github.com/miekg/dns v1.1.35 // indirect
	github.com/mitchellh/mapstructure v1.4.1
	github.com/moul/http2curl v1.0.0 // indirect
	github.com/nacos-group/nacos-sdk-go v1.0.8
	github.com/nats-io/nats-server/v2 v2.2.1 // indirect
	github.com/nats-io/nats-streaming-server v0.21.2 // indirect
	github.com/nats-io/nats.go v1.12.0
	github.com/nats-io/stan.go v0.8.3
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/open-policy-agent/opa v0.23.2
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/robfig/cron/v3 v3.0.1
	github.com/russross/blackfriday v2.0.0+incompatible // indirect
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da
	github.com/sendgrid/rest v2.6.3+incompatible // indirect
	github.com/sendgrid/sendgrid-go v3.5.0+incompatible
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.7.0
	github.com/supplyon/gremcos v0.1.0
	github.com/tidwall/gjson v1.8.0 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/valyala/fasthttp v1.21.0
	github.com/vmware/vmware-go-kcl v0.0.0-20191104173950-b6c74c3fe74e
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/yalp/jsonpath v0.0.0-20180802001716-5cc68e5049a0 // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yuin/gopher-lua v0.0.0-20200603152657-dc2b0ca8b37e // indirect
	go.mongodb.org/mongo-driver v1.5.1
	go.opencensus.io v0.22.5 // indirect
	go.uber.org/atomic v1.8.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.18.1 // indirect
	goji.io v2.0.2+incompatible // indirect
	golang.org/x/crypto v0.0.0-20210616213533-5ff15b29337e
	golang.org/x/net v0.0.0-20210614182718-04defd469f4e
	golang.org/x/oauth2 v0.0.0-20210220000619-9bb904979d93
	golang.org/x/sys v0.0.0-20210616094352-59db8d763f22 // indirect
	google.golang.org/api v0.32.0
	google.golang.org/genproto v0.0.0-20201204160425-06b3db808446
	google.golang.org/grpc v1.36.0
	gopkg.in/alexcesaro/quotedprintable.v3 v3.0.0-20150716171945-2caba252f4dc // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/couchbase/gocb.v1 v1.6.4
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.0 // indirect
	gopkg.in/gomail.v2 v2.0.0-20160411212932-81ebce5c23df
	gopkg.in/gorethink/gorethink.v4 v4.1.0 // indirect
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
	gopkg.in/kataras/go-serializer.v0 v0.0.4 // indirect
	gopkg.in/square/go-jose.v2 v2.5.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	k8s.io/api v0.20.0
	k8s.io/apiextensions-apiserver v0.20.0
	k8s.io/apimachinery v0.20.0
	k8s.io/client-go v0.20.0
)

replace k8s.io/client => github.com/kubernetes-client/go v0.0.0-20190928040339-c757968c4c36

replace github.com/dapr/components-contrib => ../components-contrib
