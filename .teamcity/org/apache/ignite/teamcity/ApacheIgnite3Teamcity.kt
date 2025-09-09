package org.apache.ignite.teamcity

import test.template_types.GradleModule
import test.template_types.TestConfiguration
import test.template_types.Tests


class ApacheIgnite3Teamcity {
    companion object {
        /*************
         * CONSTANTS *
         *************/

        /**
         * Project GradleModules with settings
         */
        private val IGNITE__AFFINITY = GradleModule(
            "Affinity",
            "ignite-affinity"
        )
        private val IGNITE__API = GradleModule(
            "API",
            "ignite-api"
        )
        private val IGNITE__BINARY_TUPLE = GradleModule(
            "Binary Tuple",
            "ignite-binary-tuple"
        )
        private val IGNITE__CLI = GradleModule(
            "CLI",
            "ignite-cli"
        )
        private val IGNITE__CLIENT = GradleModule(
            "Client",
            "ignite-client"
        )
        private val IGNITE__CLIENT_COMMON = GradleModule(
            "Client Common",
            "ignite-client-common"
        )
        private val IGNITE__CLIENT_HANDLER = GradleModule(
            "Client Handler",
            "ignite-client-handler"
        )
        private val IGNITE__CLUSTER_MANAGEMENT = GradleModule(
            "Cluster Management",
            "ignite-cluster-management"
        )
        private val IGNITE__COMPUTE = GradleModule(
            "Compute",
            "ignite-compute"
        )
        private val IGNITE__CONFIGURATION = GradleModule(
            "Configuration",
            "ignite-configuration"
        )
        private val IGNITE__CONFIGURATION_ANNOTATION_PROCESSOR = GradleModule(
            "Configuration Annotation Processor",
            "ignite-configuration-annotation-processor"
        )
        private val IGNITE__CORE = GradleModule(
            "Ignite Core",
            "ignite-core"
        )
        private val IGNITE__FILE_IO = GradleModule(
            "File IO",
            "ignite-file-io"
        )
        private val IGNITE__INDEX = GradleModule(
            "Index",
            "ignite-index"
        )
        private val IGNITE__MARSHALLER_COMMON = GradleModule(
            "Marshaller Common",
            "ignite-marshaller-common"
        )
        private val IGNITE__METASTORAGE = GradleModule(
            "Metastorage",
            "ignite-metastorage"
        )
        private val IGNITE__METASTORAGE__API = GradleModule(
            "Metastorage API",
            "ignite-metastorage-api"
        )
        private val IGNITE__METRICS = GradleModule(
            "Metrics",
            "ignite-metrics"
        )
        private val IGNITE__NETWORK = GradleModule(
            "Network",
            "ignite-network"
        )
        private val IGNITE__NETWORK_API = GradleModule(
            "Network API",
            "ignite-network-api"
        )
        private val IGNITE__PAGE_MEMORY = GradleModule(
            "Page Memory",
            "ignite-page-memory"
        )
        private val IGNITE__RAFT = GradleModule(
            "Raft",
            "ignite-raft"
        )
        private val IGNITE__REST = GradleModule(
            "Rest",
            "ignite-rest"
        )
        private val IGNITE__REST_API = GradleModule(
            "Rest API",
            "ignite-rest-api"
        )
        private val IGNITE__RUNNER = GradleModule(
            "Runner",
            "ignite-runner",
            "-XX:MaxDirectMemorySize=256m"
        )
        private val IGNITE__SCHEMA = GradleModule(
            "Schema",
            "ignite-schema"
        )
        private val IGNITE__SQL_ENGINE = GradleModule(
            "SQL Engine",
            "ignite-sql-engine"
        )
        private val IGNITE__STORAGE_API = GradleModule(
            "Storage API",
            "ignite-storage-api"
        )
        private val IGNITE__STORAGE_PAGE_MEMORY = GradleModule(
            "Storage Page Memory",
            "ignite-storage-page-memory"
        )
        private val IGNITE__STORAGE_ROCKSDB = GradleModule(
            "Storage RocksDB",
            "ignite-storage-rocksdb"
        )
        private val IGNITE__TABLE = GradleModule(
            "Table",
            "ignite-table"
        )
        private val IGNITE__TRANSACTIONS = GradleModule(
            "Transactions",
            "ignite-transactions"
        )
        private val IGNITE__VAULT = GradleModule(
            "Vault",
            "ignite-vault"
        )

        /**
         * List of GradleModules with Unit tests
         */
        private val unitTestGradleModuleList: List<GradleModule> = listOf(
            IGNITE__AFFINITY,
            IGNITE__API,
            IGNITE__BINARY_TUPLE,
            IGNITE__CLI,
            IGNITE__CLIENT,
            IGNITE__CLIENT_COMMON,
            IGNITE__CLIENT_HANDLER,
            IGNITE__CLUSTER_MANAGEMENT,
            IGNITE__COMPUTE,
            IGNITE__CONFIGURATION,
            IGNITE__CONFIGURATION_ANNOTATION_PROCESSOR,
            IGNITE__CORE,
            IGNITE__FILE_IO,
            IGNITE__INDEX,
            IGNITE__MARSHALLER_COMMON,
            IGNITE__METASTORAGE,
            IGNITE__METASTORAGE__API,
            IGNITE__METRICS,
            IGNITE__NETWORK,
            IGNITE__NETWORK_API,
            IGNITE__PAGE_MEMORY,
            IGNITE__RAFT,
            IGNITE__REST,
            IGNITE__REST_API,
            IGNITE__RUNNER,
            IGNITE__SCHEMA,
            IGNITE__SQL_ENGINE,
            IGNITE__STORAGE_API,
            IGNITE__STORAGE_PAGE_MEMORY,
            IGNITE__STORAGE_ROCKSDB,
            IGNITE__TABLE,
            IGNITE__TRANSACTIONS,
            IGNITE__VAULT
        )

        private val integrationTestGradleModulesList: List<GradleModule> = listOf(
            IGNITE__CLI,
            IGNITE__CLIENT_HANDLER,
            IGNITE__CLUSTER_MANAGEMENT,
            IGNITE__CONFIGURATION_ANNOTATION_PROCESSOR,
            IGNITE__METASTORAGE,
            IGNITE__NETWORK,
            IGNITE__PAGE_MEMORY,
            IGNITE__RAFT,
            IGNITE__RUNNER,
            IGNITE__TABLE,
            IGNITE__TRANSACTIONS,
            IGNITE__SQL_ENGINE
        )

        /**
         * Tests type settings
         */
        val UNIT = Tests(TestConfiguration("Unit", "test"), unitTestGradleModuleList)

        //JVM_CUSTOM_ARGS to """-XX:MaxDirectMemorySize=256m""".trimIndent()
        private val integrationTestConf = TestConfiguration(
            "Integration",
            "integrationTest",
            16,
            60)

        val INTEGRATION = Tests(
            integrationTestConf,
            integrationTestGradleModulesList
        )

        private val sqlConfiguration = TestConfiguration(
            "Sql Integration",
            "sqlIntegrationTest",
            16,
            60)

        val SQL_LOGIC = Tests(
            sqlConfiguration,
            listOf(IGNITE__RUNNER)
        )
    }
}
