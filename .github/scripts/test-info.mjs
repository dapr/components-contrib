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
    },
    'bindings.azure.servicebusqueues': {
        conformance: true,
        certification: true,
        requiredSecrets: ['AzureServiceBusConnectionString'],
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
    },
    'bindings.kafka-confluent': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh confluent',
    },
    'bindings.kafka-wurstmeister': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh kafka',
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
    },
    'bindings.mqtt3-mosquitto': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh mosquitto',
    },
    'bindings.mqtt3-vernemq': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh vernemq',
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
    },
    'bindings.redis.v6': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redisjson redis',
    },
    'bindings.redis.v7': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redis7 redis',
    },
    'configuration.redis.v6': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redisjson redis',
    },
    'configuration.redis.v7': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redis7 redis',
    },
    'configuration.redis': {
        certification: true,
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
    'pubsub.aws.snssqs.docker': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh snssqs',
    },
    'pubsub.aws.snssqs.terraform': {
        conformance: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        conformanceSetup: 'conformance-pubsub.aws.snssqs.terraform-setup.sh',
        conformanceDestroy:
            'conformance-pubsub.aws.snssqs.terraform-destroy.sh',
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
    },
    'pubsub.azure.servicebus.queues': {
        conformance: true,
        requiredSecrets: ['AzureServiceBusConnectionString'],
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
    },
    'pubsub.kafka-confluent': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh confluent',
    },
    'pubsub.kafka-wurstmeister': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh kafka',
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
    },
    'pubsub.mqtt3-vernemq': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh vernemq',
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
    },
    'secretstores.azure.keyvault.serviceprincipal': {
        conformance: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultTenantId',
            'AzureKeyVaultServicePrincipalClientId',
            'AzureKeyVaultServicePrincipalClientSecret',
        ],
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
    'state.aws.dynamodb.terraform': {
        conformance: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        conformanceSetup: 'conformance-state.aws.dynamodb-setup.sh',
        conformanceDestroy: 'conformance-state.aws.dynamodb-destroy.sh',
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
    },
    'state.azure.tablestorage.storage': {
        conformance: true,
        requiredSecrets: [
            'AzureBlobStorageAccessKey',
            'AzureBlobStorageAccount',
        ],
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
    },
    'state.mysql.mariadb': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh mariadb',
    },
    'state.mysql.mysql': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh mysql',
    },
    'state.oracledatabase': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh oracledatabase',
    },
    'state.postgresql': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh postgresql',
    },
    'state.redis': {
        certification: true,
    },
    'state.redis.v6': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redisjson redis',
    },
    'state.redis.v7': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh redis7 redis',
    },
    'state.rethinkdb': {
        conformance: true,
        conformanceSetup: 'docker-compose.sh rethinkdb',
    },
    'state.sqlite': {
        conformance: true,
        certification: true,
    },
    'state.sqlserver': {
        conformance: true,
        certification: true,
        conformanceSetup: 'docker-compose.sh sqlserver',
        requiredSecrets: ['AzureSqlServerConnectionString'],
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

const matrixObj = GenerateMatrix(argv[2], argv[3] == 'true')
console.log('Generated matrix:\n\n' + JSON.stringify(matrixObj, null, '  '))

writeFileSync(env.GITHUB_OUTPUT, 'test-matrix=' + JSON.stringify(matrixObj))
