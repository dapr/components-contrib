module github.com/dapr/components-contrib

go 1.18

require (
	cloud.google.com/go v0.100.2 // indirect
	cloud.google.com/go/datastore v1.1.0
	cloud.google.com/go/pubsub v1.12.2
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-amqp-common-go/v3 v3.2.3
	github.com/Azure/azure-event-hubs-go/v3 v3.3.18
	github.com/Azure/azure-sdk-for-go v65.0.0+incompatible
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/data/aztables v1.0.1
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets v0.7.1
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.0.1
	github.com/Azure/azure-service-bus-go v0.10.10
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.17.4
	github.com/Azure/go-autorest/autorest v0.11.27
	github.com/Azure/go-autorest/autorest/adal v0.9.18
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.11
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.23.1
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/a8m/documentdb v1.3.1-0.20220405205223-5b41ba0aaeb1
	github.com/aerospike/aerospike-client-go v4.5.0+incompatible
	github.com/agrea/ptr v0.0.0-20180711073057-77a518d99b7b
	github.com/ajg/form v1.5.1 // indirect
	github.com/alibaba/sentinel-golang v1.0.4
	github.com/alicebob/miniredis/v2 v2.13.3
	github.com/aliyun/aliyun-oss-go-sdk v2.0.7+incompatible
	github.com/aliyun/aliyun-tablestore-go-sdk v1.6.0
	github.com/andybalholm/brotli v1.0.2 // indirect
	github.com/apache/rocketmq-client-go/v2 v2.1.1-rc2
	github.com/apache/thrift v0.14.0 // indirect
	github.com/asaskevich/EventBus v0.0.0-20200907212545-49d423059eef
	github.com/aws/aws-sdk-go v1.41.7
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/camunda/zeebe/clients/go/v8 v8.0.4
	github.com/cenkalti/backoff/v4 v4.1.2
	github.com/cinience/go_rocketmq v0.0.2
	github.com/coreos/go-oidc v2.1.0+incompatible
	github.com/cyphar/filepath-securejoin v0.2.3
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
	github.com/fatih/color v1.13.0 // indirect
	github.com/gavv/httpexpect v2.0.0+incompatible // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/go-errors/errors v1.4.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gocql/gocql v0.0.0-20210515062232-b7ef815b4556
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/mock v1.6.0
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grandcat/zeroconf v0.0.0-20190424104450-85eadb44205c
	github.com/hashicorp/consul/api v1.11.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hazelcast/hazelcast-go-client v0.0.0-20190530123621-6cf767c2f31a
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/imkira/go-interpol v1.1.0 // indirect
	github.com/influxdata/influxdb-client-go v1.4.0
	github.com/influxdata/line-protocol v0.0.0-20210311194329-9aa0e372d097 // indirect
	github.com/jackc/pgx/v4 v4.15.0
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12
	github.com/kataras/go-errors v0.0.3 // indirect
	github.com/kataras/go-serializer v0.0.4 // indirect
	github.com/machinebox/graphql v0.2.2
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/matryer/is v1.4.0 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/microcosm-cc/bluemonday v1.0.7 // indirect
	github.com/miekg/dns v1.1.43 // indirect
	github.com/mitchellh/mapstructure v1.5.0
	github.com/moul/http2curl v1.0.0 // indirect
	github.com/nats-io/nats-server/v2 v2.7.4
	github.com/nats-io/nats-streaming-server v0.21.2 // indirect
	github.com/nats-io/nats.go v1.13.1-0.20220308171302-2f2f6968e98d
	github.com/nats-io/stan.go v0.8.3
	github.com/open-policy-agent/opa v0.40.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/robfig/cron/v3 v3.0.1
	github.com/russross/blackfriday v2.0.0+incompatible // indirect
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da
	github.com/sendgrid/rest v2.6.9+incompatible // indirect
	github.com/sendgrid/sendgrid-go v3.11.1+incompatible
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/stretchr/testify v1.7.1
	github.com/supplyon/gremcos v0.1.0
	github.com/tidwall/gjson v1.8.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/valyala/fasthttp v1.31.1-0.20211216042702-258a4c17b4f4
	github.com/vmware/vmware-go-kcl v1.5.0
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/yalp/jsonpath v0.0.0-20180802001716-5cc68e5049a0 // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yuin/gopher-lua v0.0.0-20200603152657-dc2b0ca8b37e // indirect
	go.mongodb.org/mongo-driver v1.5.1
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/crypto v0.0.0-20220511200225-c6db032c6c88
	golang.org/x/net v0.0.0-20220425223048-2871e0cb64e4
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a
	google.golang.org/api v0.74.0
	google.golang.org/genproto v0.0.0-20220405205423-9d709892a2bf
	google.golang.org/grpc v1.47.0
	gopkg.in/alexcesaro/quotedprintable.v3 v3.0.0-20150716171945-2caba252f4dc // indirect
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	gopkg.in/gomail.v2 v2.0.0-20160411212932-81ebce5c23df
	gopkg.in/gorethink/gorethink.v4 v4.1.0 // indirect
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
	gopkg.in/kataras/go-serializer.v0 v0.0.4 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	k8s.io/api v0.23.0
	k8s.io/apiextensions-apiserver v0.23.0
	k8s.io/apimachinery v0.23.0
	k8s.io/client-go v0.23.0
)

require (
	github.com/alibabacloud-go/darabonba-openapi v0.1.16
	github.com/alibabacloud-go/oos-20190601 v1.0.1
	github.com/alibabacloud-go/tea v1.1.17
	github.com/oracle/oci-go-sdk/v54 v54.0.0
	github.com/sijms/go-ora/v2 v2.2.22
)

require (
	github.com/mrz1836/postmark v1.2.9
	github.com/nats-io/nkeys v0.3.0
)

require (
	cloud.google.com/go/secretmanager v1.4.0
	dubbo.apache.org/dubbo-go/v3 v3.0.3-0.20220610080020-48691a404537
	github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig v0.4.0
	github.com/apache/dubbo-go-hessian2 v1.11.0
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.21.12+incompatible
	github.com/huaweicloud/huaweicloud-sdk-go-v3 v0.0.87
	github.com/labd/commercetools-go-sdk v0.3.2
	github.com/nacos-group/nacos-sdk-go/v2 v2.0.1
	github.com/rabbitmq/amqp091-go v1.3.4
	go.uber.org/ratelimit v0.2.0
	gopkg.in/couchbase/gocb.v1 v1.6.4
)

require gopkg.in/couchbaselabs/jsonx.v1 v1.0.1 // indirect

require (
	cloud.google.com/go/compute v1.5.0 // indirect
	cloud.google.com/go/iam v0.3.0 // indirect
	cloud.google.com/go/kms v1.4.0 // indirect
	contrib.go.opencensus.io/exporter/prometheus v0.4.1 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v0.5.1 // indirect
	github.com/RoaringBitmap/roaring v1.1.0 // indirect
	github.com/Workiva/go-datastructures v1.0.52 // indirect
	github.com/afex/hystrix-go v0.0.0-20180502004556-fa1af6a1f4f5 // indirect
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/apache/dubbo-getty v1.4.9-0.20220610060150-8af010f3f3dc // indirect
	github.com/appscode/go-querystring v0.0.0-20170504095604-0126cfb3f1dc // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/creasty/defaults v1.5.2 // indirect
	github.com/dubbogo/gost v1.11.25 // indirect
	github.com/dubbogo/triple v1.1.8 // indirect
	github.com/go-kit/log v0.1.0 // indirect
	github.com/go-logfmt/logfmt v0.5.0 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.11.0 // indirect
	github.com/hashicorp/go-hclog v1.0.0 // indirect
	github.com/jinzhu/copier v0.3.5 // indirect
	github.com/k0kubun/pp v3.0.1+incompatible // indirect
	github.com/knadh/koanf v1.4.1 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-testing-interface v1.14.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
	github.com/pelletier/go-toml v1.9.4 // indirect
	github.com/prometheus/statsd_exporter v0.21.0 // indirect
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b // indirect
	github.com/shirou/gopsutil v3.20.11+incompatible // indirect
	github.com/spf13/viper v1.10.0 // indirect
	github.com/stathat/consistent v1.0.0 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65 // indirect
	sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6 // indirect
)

require (
	github.com/99designs/keyring v1.2.0 // indirect
	github.com/AthenZ/athenz v1.10.39 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.0.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/internal v0.5.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.5 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/OneOfOne/xxhash v1.2.8 // indirect
	github.com/alibabacloud-go/alibabacloud-gateway-spi v0.0.4 // indirect
	github.com/alibabacloud-go/debug v0.0.0-20190504072949-9472017b5c68 // indirect
	github.com/alibabacloud-go/endpoint-util v1.1.0 // indirect
	github.com/alibabacloud-go/openapi-util v0.0.10 // indirect
	github.com/alibabacloud-go/tea-utils v1.4.3 // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/aliyun/alibaba-cloud-sdk-go v1.61.18 // indirect
	github.com/aliyun/credentials-go v1.1.2 // indirect
	github.com/aliyunmq/mq-http-go-sdk v1.0.3 // indirect
	github.com/apache/pulsar-client-go v0.8.1
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20220120090717-25e59572242e // indirect
	github.com/apache/rocketmq-client-go v1.2.5 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200108200545-475eaeb16496 // indirect
	github.com/awslabs/kinesis-aggregation/go v0.0.0-20210630091500-54e17340d32f // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/devigned/tab v0.1.1 // indirect
	github.com/dghubble/sling v1.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/dvsekhvalnov/jose2go v1.5.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/emirpasic/gods v1.12.0 // indirect
	github.com/go-ozzo/ozzo-validation/v4 v4.3.0 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/gogap/errors v0.0.0-20200228125012-531a6449b28c // indirect
	github.com/gogap/stack v0.0.0-20150131034635-fef68dddd4f8 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.2.0
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.2.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/serf v0.9.6 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.11.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.2.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.10.0 // indirect
	github.com/jackc/puddle v1.2.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/klauspost/compress v1.14.4 // indirect
	github.com/linkedin/goavro/v2 v2.9.8 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/nats-io/jwt/v2 v2.2.1-0.20220113022732-58e87895b296 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pkg/browser v0.0.0-20210115035449-ce105d075bb4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.12.2 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/rs/zerolog v1.25.0 // indirect
	github.com/shirou/gopsutil/v3 v3.21.6 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/sony/gobreaker v0.4.2-0.20210216022020-dd874f9dd33b // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/tetratelabs/wazero v0.0.0-20220425003459-ad61d9a6ff43
	github.com/tidwall/match v1.0.3 // indirect
	github.com/tjfoc/gmsm v1.3.2 // indirect
	github.com/tklauser/go-sysconf v0.3.6 // indirect
	github.com/tklauser/numcpus v0.2.2 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.0.2
	github.com/xdg-go/stringprep v1.0.2 // indirect
	github.com/yashtewari/glob-intersection v0.1.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20220412211240-33da011f77ad // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/fatih/pool.v2 v2.0.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.66.2 // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/klog/v2 v2.30.0 // indirect
	k8s.io/utils v0.0.0-20210930125809-cb0fa318a74b // indirect
	nhooyr.io/websocket v1.8.7 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.0 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace go.opentelemetry.io/otel => go.opentelemetry.io/otel v0.20.0

replace k8s.io/client => github.com/kubernetes-client/go v0.0.0-20190928040339-c757968c4c36

replace github.com/eclipse/paho.mqtt.golang => github.com/shivamkm07/paho.mqtt.golang v1.3.6-0.20220106130409-e28a1db639f8

replace github.com/gobwas/pool => github.com/gobwas/pool v0.2.1

replace github.com/toolkits/concurrent => github.com/niean/gotools v0.0.0-20151221085310-ff3f51fc5c60
