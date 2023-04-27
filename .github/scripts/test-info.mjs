import { argv, env, exit } from 'node:process'
import { writeFileSync } from 'node:fs'

/**
 * List of all components
 * @type {Record<string,ComponentTestProperties>}
 */
const components = {
    'bindings.azure.blobstorage': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureBlobStorageAccount',
            'AzureBlobStorageAccessKey',
            'AzureBlobStorageContainer',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
        ],
        sourcePkg: [
            'bindings/azure/blobstorage',
            'internal/component/azure/blobstorage',
        ],
    },
    'bindings.azure.cosmosdb': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureCosmosDB',
            'AzureCosmosDBMasterKey',
            'AzureCosmosDBUrl',
            'AzureCosmosDB',
            'AzureCosmosDBCollection',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
        ],
    },
    'bindings.azure.eventgrid': {
        conformance: true,
        requiredSecrets: [
            'AzureEventGridNgrokToken',
            'AzureEventGridAccessKey',
            'AzureEventGridTopicEndpoint',
            'AzureEventGridScope',
            'AzureEventGridClientSecret',
            'AzureEventGridClientId',
            'AzureEventGridTenantId',
            'AzureEventGridSubscriptionId',
        ],
        conformanceSetup: 'conformance-bindings.azure.eventgrid-setup.sh',
        conformanceDestroy: 'conformance-bindings.azure.eventgrid-destroy.sh',
    },
    'bindings.azure.eventhubs': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureEventHubsBindingsConnectionString',
            'AzureBlobStorageAccount',
            'AzureBlobStorageAccessKey',
            'AzureEventHubsBindingsHub',
            'AzureEventHubsBindingsNamespace',
            'AzureEventHubsBindingsConsumerGroup',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientSecret',
            'AzureResourceGroupName',
            'AzureCertificationSubscriptionId',
            'AzureEventHubsBindingsContainer',
            'AzureIotHubEventHubConnectionString',
            'AzureIotHubName',
            'AzureIotHubBindingsConsumerGroup',
        ],
        sourcePkg: [
            'bindings/azure/eventhubs',
            'internal/component/azure/eventhubs',
        ],
    },
    'bindings.azure.servicebusqueues': {
        conformance: true,
        certification: true,
        requiredSecrets: ['AzureServiceBusConnectionString'],
        sourcePkg: [
            'bindings/azure/servicebusqueues',
            'internal/component/azure/servicebus',
        ],
    },
    'bindings.azure.storagequeues': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureBlobStorageAccessKey',
            'AzureBlobStorageAccount',
            'AzureBlobStorageQueue',
        ],
    },
    'bindings.aws.s3': {
        certification: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        certificationSetup: 'certification-bindings.aws.s3-setup.sh',
        certificationDestroy: 'certification-bindings.aws.s3-destroy.sh',
    },
    // 'bindings.aws.s3.docker': {
    //     conformance: true,
    //     requireDocker: true,
    //     conformanceSetup: 'docker-compose.sh s3',
    // },
    'bindings.aws.s3.terraform': {
        conformance: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        conformanceSetup: 'conformance-bindings.aws.s3.terraform-setup.sh',
        conformanceDestroy: 'conformance-bindings.aws.s3.terraform-destroy.sh',
    },
    'bindings.cron': {
        conformance: true,
        certification: true,
    },
    'bindings.dubbo': {
        certification: true,
    },
    'bindings.http': {
        conformance: true,
    },
    'bindings.influx': {
        conformance: true,
        conformanceSetup: 'conformance-bindings.influx-setup.sh',
    },
    'bindings.kafka': {
        certification: true,
        sourcePkg: ['bindings/kafka', 'internal/component/kafka'],
    },
    'bindings.kafka-confluent': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh confluent',
        sourcePkg: ['bindings/kafka', 'internal/component/kafka'],
    },
    'bindings.kafka-wurstmeister': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh kafka',
        sourcePkg: ['bindings/kafka', 'internal/component/kafka'],
    },
    'bindings.kitex': {
        certification: true,
    },
    'bindings.kubemq': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh kubemq',
    },
    'bindings.localstorage': {
        certification: true,
    },
    'bindings.mqtt3-emqx': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh emqx',
        sourcePkg: ['bindings/mqtt3'],
    },
    'bindings.mqtt3-mosquitto': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh mosquitto',
        sourcePkg: ['bindings/mqtt3'],
    },
    'bindings.mqtt3-vernemq': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh vernemq',
        sourcePkg: ['bindings/mqtt3'],
    },
    'bindings.postgres': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh postgresql',
    },
    'bindings.rabbitmq': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh rabbitmq',
    },
    'bindings.redis': {
        certification: true,
        sourcePkg: ['bindings/redis', 'internal/component/redis'],
    },
    'bindings.redis.v6': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redisjson redis',
        sourcePkg: ['bindings/redis', 'internal/component/redis'],
    },
    'bindings.redis.v7': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redis7 redis',
        sourcePkg: ['bindings/redis', 'internal/component/redis'],
    },
    'configuration.postgres': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh postgresql',
    },
    'configuration.redis.v6': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redisjson redis',
        sourcePkg: ['configuration/redis', 'configuration/redis/internal'],
    },
    'configuration.redis.v7': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redis7 redis',
        sourcePkg: ['configuration/redis', 'configuration/redis/internal'],
    },
    'configuration.redis': {
        certification: true,
        sourcePkg: ['configuration/redis', 'configuration/redis/internal'],
    },
    'crypto.azure.keyvault': {
        conformance: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultTenantId',
            'AzureKeyVaultServicePrincipalClientId',
            'AzureKeyVaultServicePrincipalClientSecret',
        ],
    },
    'crypto.localstorage': {
        conformance: true,
    },
    'crypto.jwks': {
        conformance: true,
    },
    'middleware.http.bearer': {
        certification: true,
    },
    'middleware.http.ratelimit': {
        certification: true,
    },
    'pubsub.aws.snssqs': {
        certification: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        certificationSetup: 'certification-pubsub.aws.snssqs-setup.sh',
        certificationDestroy: 'certification-pubsub.aws.snssqs-destroy.sh',
    },
    // 'pubsub.aws.snssqs.docker': {
    //     conformance: true,
    //     requireDocker: true,
    //     conformanceSetup: 'docker-compose.sh snssqs',
    //     sourcePkg: 'pubsub/aws/snssqs',
    // },
    'pubsub.aws.snssqs.terraform': {
        conformance: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        conformanceSetup: 'conformance-pubsub.aws.snssqs.terraform-setup.sh',
        conformanceDestroy:
            'conformance-pubsub.aws.snssqs.terraform-destroy.sh',
        sourcePkg: 'pubsub/aws/snssqs',
    },
    'pubsub.gcp.pubsub': {
        certification: true,
        requireTerraform: true,
        requireGCPCredentials: true,
        certificationSetup: 'certification-pubsub.gcp.pubsub-setup.sh',
        certificationDestroy: 'certification-pubsub.gcp.pubsub-destroy.sh',
    },
    // 'pubsub.gcp.pubsub.docker': {
    //     conformance: true,
    //     conformanceSetup: 'docker-compose.sh gcp-pubsub',
    // },
    'pubsub.gcp.pubsub.terraform': {
        conformance: true,
        requireTerraform: true,
        requireGCPCredentials: true,
        conformanceSetup: 'conformance-pubsub.gcp.pubsub.terraform-setup.sh',
        conformanceDestroy:
            'conformance-pubsub.gcp.pubsub.terraform-destroy.sh',
    },
    'pubsub.azure.eventhubs': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureEventHubsPubsubTopicActiveConnectionString',
            'AzureEventHubsPubsubNamespace',
            'AzureEventHubsPubsubConsumerGroup',
            'AzureEventHubsPubsubNamespaceConnectionString',
            'AzureBlobStorageAccount',
            'AzureBlobStorageAccessKey',
            'AzureEventHubsPubsubContainer',
            'AzureIotHubName',
            'AzureIotHubEventHubConnectionString',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
            'AzureResourceGroupName',
            'AzureCertificationSubscriptionId',
        ],
        sourcePkg: [
            'pubsub/azure/eventhubs',
            'internal/component/azure/eventhubs',
        ],
    },
    'pubsub.azure.servicebus.queues': {
        conformance: true,
        requiredSecrets: ['AzureServiceBusConnectionString'],
        sourcePkg: [
            'pubsub/azure/servicebus/queues',
            'internal/component/azure/servicebus',
        ],
    },
    'pubsub.azure.servicebus.topics': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureServiceBusConnectionString',
            'AzureServiceBusNamespace',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
        ],
        sourcePkg: [
            'pubsub/azure/servicebus/topics',
            'internal/component/azure/servicebus',
        ],
    },
    'pubsub.in-memory': {
        conformance: true,
    },
    'pubsub.jetstream': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh jetstream',
    },
    'pubsub.kafka': {
        certification: true,
        sourcePkg: ['pubsub/kafka', 'internal/component/kafka'],
    },
    'pubsub.kafka-confluent': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh confluent',
        sourcePkg: ['pubsub/kafka', 'internal/component/kafka'],
    },
    'pubsub.kafka-wurstmeister': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh kafka',
        sourcePkg: ['pubsub/kafka', 'internal/component/kafka'],
    },
    'pubsub.kubemq': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh kubemq',
    },
    'pubsub.mqtt3': {
        certification: true,
    },
    'pubsub.mqtt3-emqx': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh emqx',
        sourcePkg: ['pubsub/mqtt3'],
    },
    'pubsub.mqtt3-vernemq': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh vernemq',
        sourcePkg: ['pubsub/mqtt3'],
    },
    'pubsub.natsstreaming': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh natsstreaming',
    },
    'pubsub.pulsar': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh pulsar',
    },
    'pubsub.rabbitmq': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh rabbitmq',
    },
    'pubsub.redis.v6': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redisjson redis',
        sourcePkg: ['pubsub/redis', 'internal/component/redis'],
    },
    // This test is currently disabled due to issues with Redis v7
    /*'pubsub.redis.v7': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redis7 redis',
    },*/
    'pubsub.solace': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh solace',
    },
    'secretstores.azure.keyvault': {
        certification: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultTenantId',
            'AzureKeyVaultClientId',
            'AzureKeyVaultServicePrincipalClientId',
            'AzureKeyVaultServicePrincipalClientSecret',
            'AzureContainerRegistryName',
            'AzureResourceGroupName',
        ],
        requiredCerts: ['AzureKeyVaultCert'],
    },
    'secretstores.azure.keyvault.certificate': {
        conformance: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultTenantId',
            'AzureKeyVaultClientId',
        ],
        requiredCerts: ['AzureKeyVaultCert'],
        sourcePkg: 'secretstores/azure/keyvault',
    },
    'secretstores.azure.keyvault.serviceprincipal': {
        conformance: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultTenantId',
            'AzureKeyVaultServicePrincipalClientId',
            'AzureKeyVaultServicePrincipalClientSecret',
        ],
        sourcePkg: 'secretstores/azure/keyvault',
    },
    'secretstores.hashicorp.vault': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh hashicorp-vault vault',
    },
    'secretstores.kubernetes': {
        conformance: true,
        requireKind: true,
        conformanceSetup: 'conformance-secretstores.kubernetes-setup.sh',
    },
    'secretstores.local.env': {
        conformance: true,
        certification: true,
    },
    'secretstores.local.file': {
        conformance: true,
        certification: true,
    },
    'state.aws.dynamodb': {
        certification: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        certificationSetup: 'certification-state.aws.dynamodb-setup.sh',
        certificationDestroy: 'certification-state.aws.dynamodb-destroy.sh',
    },
    // 'state.aws.dynamodb.docker': {
    //     conformance: true,
    //     requireDocker: true,
    //     conformanceSetup: 'docker-compose.sh dynamodb',
    // },
    'state.aws.dynamodb.terraform': {
        conformance: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        conformanceSetup: 'conformance-state.aws.dynamodb-setup.sh',
        conformanceDestroy: 'conformance-state.aws.dynamodb-destroy.sh',
        sourcePkg: 'state/aws/dynamodb',
    },
    'state.azure.blobstorage': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureBlobStorageAccount',
            'AzureBlobStorageAccessKey',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
            'AzureBlobStorageContainer',
        ],
        sourcePkg: [
            'state/azure/blobstorage',
            'internal/component/azure/blobstorage',
        ],
    },
    'state.azure.cosmosdb': {
        conformance: true,
        certification: true,
        requiredSecrets: [
            'AzureCosmosDBMasterKey',
            'AzureCosmosDBUrl',
            'AzureCosmosDB',
            'AzureCosmosDBCollection',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
        ],
    },
    'state.azure.sql': {
        conformance: true,
        requiredSecrets: [
            'AzureResourceGroupName',
            'AzureSqlServerName',
            'AzureSqlServerConnectionString',
        ],
        conformanceSetup: 'conformance-state.azure.sql-setup.sh',
        conformanceDestroy: 'conformance-state.azure.sql-destroy.sh',
    },
    'state.azure.tablestorage': {
        certification: true,
        requiredSecrets: [
            'AzureBlobStorageAccount',
            'AzureBlobStorageAccessKey',
            'AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
        ],
    },
    'state.azure.tablestorage.cosmosdb': {
        conformance: true,
        requiredSecrets: [
            'AzureCosmosDBTableAPI',
            'AzureCosmosDBTableAPIMasterKey',
        ],
        sourcePkg: ['state/azure/tablestorage'],
    },
    'state.azure.tablestorage.storage': {
        conformance: true,
        requiredSecrets: [
            'AzureBlobStorageAccessKey',
            'AzureBlobStorageAccount',
        ],
        sourcePkg: ['state/azure/tablestorage'],
    },
    'state.cassandra': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh cassandra',
    },
    'state.cloudflare.workerskv': {
        conformance: true,
        requireCloudflareCredentials: true,
        nodeJsVersion: '18.x',
        conformanceSetup: 'conformance-state.cloudflare.workerskv-setup.sh',
        conformanceDestroy: 'conformance-state.cloudflare.workerskv-destroy.sh',
    },
    'state.cockroachdb': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh cockroachdb',
        sourcePkg: [
            'state/cockroachdb',
            'internal/component/postgresql',
            'internal/component/sql',
        ],
    },
    'state.etcd': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh etcd',
    },
    'state.in-memory': {
        conformance: true,
    },
    'state.memcached': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh memcached',
    },
    'state.mongodb': {
        conformance: true,
        certification: true,
        mongoDbVersion: '4.2',
    },
    'state.mysql': {
        certification: true,
        sourcePkg: ['state/mysql', 'internal/component/sql'],
    },
    'state.mysql.mariadb': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh mariadb',
        sourcePkg: ['state/mysql', 'internal/component/sql'],
    },
    'state.mysql.mysql': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh mysql',
        sourcePkg: ['state/mysql', 'internal/component/sql'],
    },
    'state.oracledatabase': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh oracledatabase',
    },
    'state.postgresql': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh postgresql',
        sourcePkg: [
            'state/postgresql',
            'internal/component/postgresql',
            'internal/component/sql',
        ],
    },
    'state.redis': {
        certification: true,
        sourcePkg: ['state/redis', 'internal/component/redis'],
    },
    'state.redis.v6': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redisjson redis',
        sourcePkg: ['state/redis', 'internal/component/redis'],
    },
    'state.redis.v7': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redis7 redis',
        sourcePkg: ['state/redis', 'internal/component/redis'],
    },
    'state.rethinkdb': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh rethinkdb',
    },
    'state.sqlite': {
        conformance: true,
        certification: true,
        sourcePkg: ['state/sqlite', 'internal/component/sql'],
    },
    'state.sqlserver': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh sqlserver',
        requiredSecrets: ['AzureSqlServerConnectionString'],
        sourcePkg: ['state/sqlserver', 'internal/component/sql'],
    },
    // 'state.gcp.firestore.docker': {
    //     conformance: true,
    //     requireDocker: true,
    //     conformanceSetup: 'docker-compose.sh gcpfirestore',
    // },
    'state.gcp.firestore.cloud': {
        conformance: true,
        requireGCPCredentials: true,
        conformanceSetup: 'conformance-state.gcp.firestore-setup.sh',
    },
    'state.gcp.firestore': {
        certification: true,
        requireGCPCredentials: true,
        certificationSetup: 'certification-state.gcp.firestore-setup.sh',
    },
    'workflows.temporal': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh temporal',
    },
}

/**
 * Type for the objects in the components dictionary
 * @typedef {Object} ComponentTestProperties
 * @property {boolean?} conformance If true, enables for conformance tests
 * @property {boolean?} certification If true, enables for certification tests
 * @property {string[]?} requiredSecrets Required secrets (if not empty, test becomes "cloud-only")
 * @property {string[]?} requiredCerts Required certs (if not empty, test becomes "cloud-only")
 * @property {boolean?} requireAWSCredentials If true, requires AWS credentials and makes the test "cloud-only"
 * @property {boolean?} requireGCPCredentials If true, requires GCP credentials and makes the test "cloud-only"
 * @property {boolean?} requireCloudflareCredentials If true, requires Cloudflare credentials and makes the test "cloud-only"
 * @property {boolean?} requireTerraform If true, requires Terraform
 * @property {boolean?} requireKind If true, requires KinD
 * @property {string?} conformanceSetup Setup script for conformance tests
 * @property {string?} conformanceDestroy Destroy script for conformance tests
 * @property {string?} certificationSetup Setup script for certification tests
 * @property {string?} certificationDestroy Destroy script for certification tests
 * @property {string?} nodeJsVersion If set, installs the specified Node.js version
 * @property {string?} mongoDbVersion If set, installs the specified MongoDB version
 * @property {string|string[]?} sourcePkg If set, sets the specified source package
 */

/**
 * Test matrix object
 * @typedef {Object} TestMatrixElement
 * @property {string} component Component name
 * @property {string?} required-secrets Required secrets
 * @property {string?} required-certs Required certs
 * @property {boolean?} require-aws-credentials Requires AWS credentials
 * @property {boolean?} require-gcp-credentials Requires GCP credentials
 * @property {boolean?} require-cloudflare-credentials Requires Cloudflare credentials
 * @property {boolean?} require-terraform Requires Terraform
 * @property {boolean?} require-kind Requires KinD
 * @property {string?} setup-script Setup script
 * @property {string?} destroy-script Destroy script
 * @property {string?} nodejs-version Install the specified Node.js version if set
 * @property {string?} mongodb-version Install the specified MongoDB version if set
 * @property {string?} source-pkg Source package
 */

/**
 * Returns the list of components for the matrix.
 * @param {'conformance'|'certification'} testKind Kind of test
 * @param {boolean} enableCloudTests If true, returns components that require secrets or credentials too (which can't be used as part of the regular CI in a PR)
 * @returns {TestMatrixElement[]} Test matrix object
 */
function GenerateMatrix(testKind, enableCloudTests) {
    /** @type {TestMatrixElement[]} */
    const res = []
    for (const name in components) {
        const comp = components[name]
        if (!comp[testKind]) {
            continue
        }

        // Skip cloud-only tests if enableCloudTests is false
        if (!enableCloudTests) {
            if (
                comp.requiredSecrets?.length ||
                comp.requiredCerts?.length ||
                comp.requireAWSCredentials ||
                comp.requireGCPCredentials ||
                comp.requireCloudflareCredentials
            ) {
                continue
            }
        } else {
            // For conformance tests, avoid running Docker and Cloud Tests together.
            if (comp.conformance && comp.requireDocker) {
                continue
            }
        }

        if (comp.sourcePkg) {
            // Ensure it's an array
            if (!Array.isArray(comp.sourcePkg)) {
                comp.sourcePkg = [comp.sourcePkg]
            }
        } else {
            // Default is to use the component name, replacing dots with /
            comp.sourcePkg = [name.replace(/\./g, '/')]
        }

        // Add the component to the array
        res.push({
            component: name,
            'required-secrets': comp.requiredSecrets?.length
                ? comp.requiredSecrets.join(',')
                : undefined,
            'required-certs': comp.requiredCerts?.length
                ? comp.requiredCerts.join(',')
                : undefined,
            'require-aws-credentials': comp.requireAWSCredentials
                ? 'true'
                : undefined,
            'require-gcp-credentials': comp.requireGCPCredentials
                ? 'true'
                : undefined,
            'require-cloudflare-credentials': comp.requireCloudflareCredentials
                ? 'true'
                : undefined,
            'require-terraform': comp.requireTerraform ? 'true' : undefined,
            'require-kind': comp.requireKind ? 'true' : undefined,
            'setup-script': comp[testKind + 'Setup'] || undefined,
            'destroy-script': comp[testKind + 'Destroy'] || undefined,
            'nodejs-version': comp.nodeJsVersion || undefined,
            'mongodb-version': comp.mongoDbVersion || undefined,
            'source-pkg': comp.sourcePkg
                .map((p) => 'github.com/dapr/components-contrib/' + p)
                .join(','),
        })
    }

    return res
}

// Upon invocation, writes the matrix to the $GITHUB_OUTPUT file
if (!env.GITHUB_OUTPUT) {
    console.error('Missing environmental variable GITHUB_OUTPUT')
    exit(1)
}
if (argv.length < 3 || !['conformance', 'certification'].includes(argv[2])) {
    console.error("First parameter must be 'conformance' or 'certification'")
    exit(1)
}
if (argv.length < 4 || !['true', 'false'].includes(argv[3])) {
    console.error("First parameter must be 'true' or 'false'")
    exit(1)
}

const testKind = argv[2]
const enableCloudTests = argv[3] == 'true'
const matrixObj = GenerateMatrix(testKind, enableCloudTests)
console.log('Generated matrix:\n\n' + JSON.stringify(matrixObj, null, '  '))

writeFileSync(env.GITHUB_OUTPUT, 'test-matrix=' + JSON.stringify(matrixObj))
