module github.com/dapr/components-contrib

go 1.19

require (
	cloud.google.com/go/datastore v1.8.0
	cloud.google.com/go/pubsub v1.26.0
	cloud.google.com/go/secretmanager v1.8.0
	cloud.google.com/go/storage v1.27.0
	dubbo.apache.org/dubbo-go/v3 v3.0.3-0.20220610080020-48691a404537
	github.com/Azure/azure-amqp-common-go/v3 v3.2.3
	github.com/Azure/azure-event-hubs-go/v3 v3.3.18
	github.com/Azure/azure-sdk-for-go v65.0.0+incompatible
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.1.4
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig v0.4.3
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v0.3.2
	github.com/Azure/azure-sdk-for-go/sdk/data/aztables v1.0.1
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets v0.10.1
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.1.1
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.17.5
	github.com/Azure/go-autorest/autorest v0.11.28
	github.com/Azure/go-autorest/autorest/adal v0.9.21
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.11
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.37.2
	github.com/aerospike/aerospike-client-go v4.5.2+incompatible
	github.com/alibaba/sentinel-golang v1.0.4
	github.com/alibabacloud-go/darabonba-openapi v0.2.1
	github.com/alibabacloud-go/oos-20190601 v1.0.4
	github.com/alibabacloud-go/tea v1.1.20
	github.com/alibabacloud-go/tea-utils v1.4.5
	github.com/alicebob/miniredis/v2 v2.23.0
	github.com/aliyun/aliyun-log-go-sdk v0.1.39
	github.com/aliyun/aliyun-oss-go-sdk v2.2.5+incompatible
	github.com/aliyun/aliyun-tablestore-go-sdk v1.7.7
	github.com/apache/dubbo-go-hessian2 v1.11.3
	github.com/apache/pulsar-client-go v0.9.0
	github.com/apache/rocketmq-client-go/v2 v2.1.0
	github.com/aws/aws-sdk-go v1.44.128
	github.com/bradfitz/gomemcache v0.0.0-20221031212613-62deef7fc822
	github.com/camunda/zeebe/clients/go/v8 v8.1.3
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/cinience/go_rocketmq v0.0.2
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/cyphar/filepath-securejoin v0.2.3
	github.com/dancannon/gorethink v4.0.0+incompatible
	github.com/dapr/kit v0.0.3-0.20221102045011-c213121f0b4f
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/dghubble/go-twitter v0.0.0-20221024160433-0cc1e72ed6d8
	github.com/dghubble/oauth1 v0.7.1
	github.com/didip/tollbooth v4.0.2+incompatible
	github.com/eclipse/paho.mqtt.golang v1.4.2
	github.com/fasthttp-contrib/sessions v0.0.0-20160905201309-74f6ac73d5d5
	github.com/ghodss/yaml v1.0.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gocql/gocql v1.2.1
	github.com/golang-jwt/jwt/v4 v4.4.2
	github.com/golang/mock v1.6.0
	github.com/google/uuid v1.3.0
	github.com/googleapis/gax-go/v2 v2.6.0
	github.com/gorilla/mux v1.8.0
	github.com/grandcat/zeroconf v1.0.0
	github.com/hashicorp/consul/api v1.13.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hazelcast/hazelcast-go-client v0.0.0-20190530123621-6cf767c2f31a
	github.com/http-wasm/http-wasm-host-go v0.1.0
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.21.12+incompatible
	github.com/huaweicloud/huaweicloud-sdk-go-v3 v0.1.6
	github.com/influxdata/influxdb-client-go v1.4.0
	github.com/jackc/pgx/v5 v5.0.4
	github.com/json-iterator/go v1.1.12
	github.com/kubemq-io/kubemq-go v1.7.6
	github.com/labd/commercetools-go-sdk v1.1.0
	github.com/machinebox/graphql v0.2.2
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4
	github.com/mrz1836/postmark v1.3.0
	github.com/nacos-group/nacos-sdk-go/v2 v2.1.2
	github.com/nats-io/nats-server/v2 v2.9.4
	github.com/nats-io/nats.go v1.19.1
	github.com/nats-io/nkeys v0.3.0
	github.com/nats-io/stan.go v0.10.3
	github.com/open-policy-agent/opa v0.45.0
	github.com/oracle/oci-go-sdk/v54 v54.0.0
	github.com/pashagolub/pgxmock/v2 v2.1.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/rabbitmq/amqp091-go v1.5.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
	github.com/sendgrid/sendgrid-go v3.12.0+incompatible
	github.com/sijms/go-ora/v2 v2.5.3
	github.com/stretchr/testify v1.8.1
	github.com/supplyon/gremcos v0.1.38
	github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common v1.0.527
	github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ssm v1.0.527
	github.com/tetratelabs/wazero v1.0.0-pre.3
	github.com/valyala/fasthttp v1.41.0
	github.com/vmware/vmware-go-kcl v1.5.0
	github.com/xdg-go/scram v1.1.1
	go.mongodb.org/mongo-driver v1.10.3
	go.temporal.io/api v1.12.0
	go.temporal.io/sdk v1.17.0
	go.uber.org/atomic v1.10.0
	go.uber.org/ratelimit v0.2.0
	golang.org/x/crypto v0.1.0
	golang.org/x/net v0.1.0
	golang.org/x/oauth2 v0.1.0
	google.golang.org/api v0.101.0
	google.golang.org/grpc v1.50.1
	gopkg.in/couchbase/gocb.v1 v1.6.7
	gopkg.in/gomail.v2 v2.0.0-20160411212932-81ebce5c23df
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.25.3
	k8s.io/apiextensions-apiserver v0.25.3
	k8s.io/apimachinery v0.25.3
	k8s.io/client-go v0.25.3
	k8s.io/utils v0.0.0-20221012122500-cfd413dd9e85
)

require (
	cloud.google.com/go v0.105.0 // indirect
	cloud.google.com/go/compute v1.10.0 // indirect
	cloud.google.com/go/iam v0.6.0 // indirect
	contrib.go.opencensus.io/exporter/prometheus v0.4.1 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.1 // indirect
	github.com/AthenZ/athenz v1.10.39 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.0.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/internal v0.7.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.5 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v0.5.1 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/OneOfOne/xxhash v1.2.8 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/RoaringBitmap/roaring v1.1.0 // indirect
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/Workiva/go-datastructures v1.0.53 // indirect
	github.com/afex/hystrix-go v0.0.0-20180502004556-fa1af6a1f4f5 // indirect
	github.com/agnivade/levenshtein v1.1.1 // indirect
	github.com/ajg/form v1.5.1 // indirect
	github.com/alibabacloud-go/alibabacloud-gateway-spi v0.0.4 // indirect
	github.com/alibabacloud-go/debug v0.0.0-20190504072949-9472017b5c68 // indirect
	github.com/alibabacloud-go/endpoint-util v1.1.0 // indirect
	github.com/alibabacloud-go/openapi-util v0.0.11 // indirect
	github.com/alibabacloud-go/tea-xml v1.1.2 // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/aliyun/alibaba-cloud-sdk-go v1.61.1704 // indirect
	github.com/aliyun/credentials-go v1.1.2 // indirect
	github.com/aliyunmq/mq-http-go-sdk v1.0.3 // indirect
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/apache/dubbo-getty v1.4.9-0.20220610060150-8af010f3f3dc // indirect
	github.com/apache/rocketmq-client-go v1.2.5 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200108200545-475eaeb16496 // indirect
	github.com/awslabs/kinesis-aggregation/go v0.0.0-20210630091500-54e17340d32f // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/clbanning/mxj/v2 v2.5.6 // indirect
	github.com/creasty/defaults v1.5.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/deepmap/oapi-codegen v1.3.6 // indirect
	github.com/devigned/tab v0.1.1 // indirect
	github.com/dghubble/sling v1.4.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/dubbogo/gost v1.11.25 // indirect
	github.com/dubbogo/triple v1.1.8 // indirect
	github.com/dvsekhvalnov/jose2go v1.5.0 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/emicklei/go-restful/v3 v3.8.0 // indirect
	github.com/emirpasic/gods v1.12.0 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/gavv/httpexpect v2.0.0+incompatible // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-kit/log v0.2.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.5 // indirect
	github.com/go-openapi/swag v0.19.14 // indirect
	github.com/go-ozzo/ozzo-validation/v4 v4.3.0 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.11.0 // indirect
	github.com/go-resty/resty/v2 v2.7.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/gogap/errors v0.0.0-20200228125012-531a6449b28c // indirect
	github.com/gogap/stack v0.0.0-20150131034635-fef68dddd4f8 // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gogo/status v1.1.1 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/google/gnostic v0.5.7-v3refs // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/serf v0.9.7 // indirect
	github.com/imdario/mergo v0.3.6 // indirect
	github.com/imkira/go-interpol v1.1.0 // indirect
	github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/puddle/v2 v2.0.0 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jinzhu/copier v0.3.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/k0kubun/pp v3.0.1+incompatible // indirect
	github.com/kataras/go-errors v0.0.3 // indirect
	github.com/kataras/go-serializer v0.0.4 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/knadh/koanf v1.4.1 // indirect
	github.com/kubemq-io/protobuf v1.3.1 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/labstack/echo/v4 v4.9.0 // indirect
	github.com/labstack/gommon v0.3.1 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/linkedin/goavro/v2 v2.9.8 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mailru/easyjson v0.7.6 // indirect
	github.com/matryer/is v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/microcosm-cc/bluemonday v1.0.21 // indirect
	github.com/miekg/dns v1.1.43 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/montanaflynn/stats v0.6.6 // indirect
	github.com/moul/http2curl v1.0.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
	github.com/nats-io/jwt/v2 v2.3.0 // indirect
	github.com/nats-io/nats-streaming-server v0.25.2 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/pelletier/go-toml v1.9.4 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pkg/browser v0.0.0-20210115035449-ce105d075bb4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/pquerna/cachecontrol v0.0.0-20171018203845-0dec1b30a021 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/prometheus/statsd_exporter v0.21.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/rs/zerolog v1.25.0 // indirect
	github.com/russross/blackfriday v1.6.0 // indirect
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b // indirect
	github.com/sendgrid/rest v2.6.9+incompatible // indirect
	github.com/shirou/gopsutil v3.20.11+incompatible // indirect
	github.com/shirou/gopsutil/v3 v3.21.6 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/sony/gobreaker v0.4.2-0.20210216022020-dd874f9dd33b // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/tchap/go-patricia/v2 v2.3.1 // indirect
	github.com/tidwall/gjson v1.13.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tjfoc/gmsm v1.3.2 // indirect
	github.com/tklauser/go-sysconf v0.3.6 // indirect
	github.com/tklauser/numcpus v0.2.2 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.3 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/yalp/jsonpath v0.0.0-20180802001716-5cc68e5049a0 // indirect
	github.com/yashtewari/glob-intersection v0.1.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yuin/gopher-lua v0.0.0-20210529063254-f4c35e4016d9 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
	golang.org/x/term v0.1.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	golang.org/x/time v0.0.0-20220922220347-f3bd1da661af // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20221027153422-115e99e71e1c // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/alexcesaro/quotedprintable.v3 v3.0.0-20150716171945-2caba252f4dc // indirect
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.1 // indirect
	gopkg.in/fatih/pool.v2 v2.0.0 // indirect
	gopkg.in/gorethink/gorethink.v4 v4.1.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.66.6 // indirect
	gopkg.in/kataras/go-serializer.v0 v0.0.4 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/square/go-jose.v2 v2.4.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/klog/v2 v2.80.1 // indirect
	k8s.io/kube-openapi v0.0.0-20220803162953-67bda5d908f1 // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
	stathat.com/c/consistent v1.0.0 // indirect
)

replace github.com/gobwas/pool => github.com/gobwas/pool v0.2.1

replace github.com/toolkits/concurrent => github.com/niean/gotools v0.0.0-20151221085310-ff3f51fc5c60

// this is a fork which addresses a performance issues due to go routines
replace dubbo.apache.org/dubbo-go/v3 => dubbo.apache.org/dubbo-go/v3 v3.0.3-0.20220610080020-48691a404537
