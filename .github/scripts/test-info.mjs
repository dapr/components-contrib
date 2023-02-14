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
    },
    'bindings.kafka': {
        certification: true,
    },
    'bindings.kafka-confluent': {
        conformance: true,
    },
    'bindings.kafka-wurstmeister': {
        conformance: true,
    },
    'bindings.kubemq': {
        conformance: true,
        conformanceSetup: 'conformance-kubemq-setup.sh'
    },
    'bindings.localstorage': {
        certification: true,
    },
    'bindings.mqtt3-emqx': {
        conformance: true,
        conformanceSetup: 'conformance-mqtt3-emqx-setup.sh'
    },
    'bindings.mqtt3-mosquitto': {
        conformance: true,
        conformanceSetup: 'conformance-mqtt3-mosquitto-setup.sh'
    },
    'bindings.mqtt3-vernemq': {
        conformance: true,
        conformanceSetup: 'conformance-mqtt3-vernemq-setup.sh'
    },
    'bindings.postgres': {
        conformance: true,
        certification: true,
    },
    'bindings.rabbitmq': {
        conformance: true,
        certification: true,
        conformanceSetup: 'conformance-rabbitmq-setup.sh'
    },
    'bindings.redis': {
        certification: true,
    },
    'bindings.redis.v6': {
        conformance: true,
        conformanceSetup: 'conformance-redisv6-setup.sh'
    },
    'bindings.redis.v7': {
        conformance: true,
        conformanceSetup: 'conformance-redisv7-setup.sh'
    },
    'configuration.redis.v6': {
        conformance: true,
        conformanceSetup: 'conformance-redisv6-setup.sh'
    },
    'configuration.redis.v7': {
        conformance: true,
        conformanceSetup: 'conformance-redisv7-setup.sh'
    },
    'pubsub.aws.snssqs': {
        certification: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        certificationSetup: 'certification-state.aws.snssqs-setup.sh',
        certificationDestroy: 'certification-state.aws.snssqs-destroy.sh',
    },
    'pubsub.aws.snssqs.docker': {
        conformance: true,
    },
    'pubsub.aws.snssqs.terraform': {
        conformance: true,
        requireAWSCredentials: true,
        requireTerraform: true,
        conformanceSetup: 'conformance-state.aws.snssqs-setup.sh',
        conformanceDestroy: 'conformance-state.aws.snssqs-destroy.sh',
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
            'zureServiceBusConnectionString',
            'AzureServiceBusNamespace',
            ' AzureCertificationTenantId',
            'AzureCertificationServicePrincipalClientId',
            'AzureCertificationServicePrincipalClientSecret',
        ],
    },
    'pubsub.hazelcast': {
        conformance: true,
        conformanceSetup: 'conformance-hazelcast-setup.sh'
    },
    'pubsub.in-memory': {
        conformance: true,
    },
    'pubsub.kafka': {
        certification: true,
    },
    'pubsub.kafka-confluent': {
        conformance: true,
    },
    'pubsub.kafka-wurstmeister': {
        conformance: true,
    },
    'pubsub.kubemq': {
        conformance: true,
        conformanceSetup: 'conformance-kubemq-setup.sh'
    },
    'pubsub.mqtt3': {
        certification: true,
    },
    'pubsub.mqtt3-emqx': {
        conformance: true,
        conformanceSetup: 'conformance-mqtt3-emqx-setup.sh'
    },
    'pubsub.mqtt3-vernemq': {
        conformance: true,
        conformanceSetup: 'conformance-mqtt3-vernemq-setup.sh'
    },
    'pubsub.natsstreaming': {
        conformance: true,
        conformanceSetup: 'conformance-natsstreaming-setup.sh'
    },
    'pubsub.pulsar': {
        conformance: true,
        certification: true,
        conformanceSetup: 'conformance-pulsar-setup.sh'
    },
    'pubsub.rabbitmq': {
        conformance: true,
        certification: true,
        conformanceSetup: 'conformance-rabbitmq-setup.sh'
    },
    'pubsub.redis.v6': {
        conformance: true,
        conformanceSetup: 'conformance-redisv6-setup.sh'
    },
    'pubsub.redis.v7': {
        conformance: true,
        conformanceSetup: 'conformance-redisv7-setup.sh'
    },
    'pubsub.solace': {
        conformance: true,
        //conformanceSetup: 'conformance-solace-setup.sh'
    },
    'secretstores.azure.keyvault': {
        certification: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultSecretStoreTenantId',
            'AzureKeyVaultSecretStoreClientId',
            'AzureKeyVaultSecretStoreServicePrincipalClientId',
            'AzureKeyVaultSecretStoreServicePrincipalClientSecret',
            'AzureContainerRegistryName',
            'AzureResourceGroupName',
        ],
        requiredCerts: ['AzureKeyVaultSecretStoreCert'],
    },
    'secretstores.azure.keyvault.certificate': {
        conformance: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultSecretStoreTenantId',
            'AzureKeyVaultSecretStoreClientId',
        ],
        requiredCerts: ['AzureKeyVaultSecretStoreCert'],
    },
    'secretstores.azure.keyvault.serviceprincipal': {
        conformance: true,
        requiredSecrets: [
            'AzureKeyVaultName',
            'AzureKeyVaultSecretStoreTenantId',
            'AzureKeyVaultSecretStoreServicePrincipalClientId',
            'AzureKeyVaultSecretStoreServicePrincipalClientSecret',
        ],
    },
    'secretstores.hashicorp.vault': {
        conformance: true,
        certification: true,
        conformanceSetup: 'conformance-hashicorp-vault-setup.sh'
    },
    'secretstores.kubernetes': {
        conformance: true,
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
    },
    'state.cloudflare.workerskv': {
        conformance: true,
        requireCloudflareCredentials: true,
    },
    'state.cockroachdb': {
        conformance: true,
        certification: true,
    },
    'state.in-memory': {
        conformance: true,
    },
    'state.memcached': {
        conformance: true,
        certification: true,
        conformanceSetup: 'conformance-memcached-setup.sh',
    },
    'state.mongodb': {
        conformance: true,
        certification: true,
    },
    'state.mysql': {
        certification: true,
    },
    'state.mysql.mariadb': {
        conformance: true,
    },
    'state.mysql.mysql': {
        conformance: true,
    },
    'state.postgresql': {
        conformance: true,
        certification: true,
    },
    'state.redis': {
        certification: true,
    },
    'state.redis.v6': {
        conformance: true,
    },
    'state.redis.v7': {
        conformance: true,
    },
    'state.rethinkdb': {
        conformance: true,
    },
    'state.sqlite': {
        conformance: true,
        certification: true,
    },
    'state.sqlserver': {
        conformance: true,
        certification: true,
        requiredSecrets: ['AzureSqlServerConnectionString'],
    },
    'workflows.temporal': {
        conformance: true,
        conformanceSetup: 'conformance-temporal-setup.sh',
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
 * @property {boolean?} requireCloudflareCredentials If true, requires Cloudflare credentials and makes the test "cloud-only"
 * @property {boolean?} requireTerraform If true, requires Terraform
 * @property {string?} conformanceSetup Setup script for conformance tests
 * @property {string?} conformanceDestroy Destroy script for conformance tests
 * @property {string?} certificationSetup Setup script for certification tests
 * @property {string?} certificationDestroy Destroy script for certification tests
 */

/**
 * Test matrix object
 * @typedef {Object} TestMatrixElement
 * @property {string} component Component name
 * @property {string?} required-secrets Required secrets
 * @property {string?} required-certs Required certs
 * @property {boolean?} require-aws-credentials Requires AWS credentials
 * @property {boolean?} require-cloudflare-credentials Requires Cloudflare credentials
 * @property {boolean?} require-terraform Requires Terraform
 * @property {string?} setup-script Setup script
 * @property {string?} destroy-script Destroy script
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
            if (comp.requiredSecrets?.length || comp.requiredCerts?.length || comp.requireAWSCredentials || comp.requireCloudflareCredentials) {
                continue
            }
        }

        // Add the component to the array
        res.push({
            component: name,
            "required-secrets": comp.requiredSecrets?.length ? comp.requiredSecrets : undefined,
            "required-certs": comp.requiredCerts?.length ? comp.requiredCerts : undefined,
            "require-aws-credentials": comp.requireAWSCredentials ? 'true' : undefined,
            "require-cloudflare-credentials": comp.requireCloudflareCredentials ? 'true' : undefined,
            "require-terraform": comp.requireTerraform ? 'true' : undefined,
            "setup-script": comp[testKind+'Setup'] || undefined,
            "destroy-script": comp[testKind+'Destroy'] || undefined,
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
console.log('Generated matrix:\n\n'+JSON.stringify(matrixObj, null, '  '))

writeFileSync(env.GITHUB_OUTPUT, JSON.stringify(matrixObj))
