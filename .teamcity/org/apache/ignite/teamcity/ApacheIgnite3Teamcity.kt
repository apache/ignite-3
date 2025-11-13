package org.apache.ignite.teamcity

import test.template_types.GradleModule
import test.template_types.RunTests
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
        private val IGNITE__ARCH_TEST = GradleModule(
            "Arch",
            "ignite-arch-test"
        )
        private val IGNITE__API = GradleModule(
            "API",
            "ignite-api"
        )
        private val IGNITE__BINARY_TUPLE = GradleModule(
            "Binary Tuple",
            "ignite-binary-tuple"
        )
        private val IGNITE__CATALOG = GradleModule(
            "Catalog",
            "ignite-catalog"
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
        private val IGNITE__CODE_DEPLOYMENT = GradleModule(
            "Code Deployment",
            "ignite-code-deployment"
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
        private val IGNITE__DISTRIBUTION_ZONES = GradleModule(
            "Distribution Zones",
            "ignite-distribution-zones"
        )
        private val IGNITE__EXAMPLES = GradleModule(
            "Examples",
            "ignite-examples"
        )
        private val IGNITE__FILE_TRANSFER = GradleModule(
            "File Transfer",
            "ignite-file-transfer"
        )
        private val IGNITE__FILE_IO = GradleModule(
            "File IO",
            "ignite-file-io"
        )
        private val IGNITE__INDEX = GradleModule(
            "Index",
            "ignite-index"
        )
        private val IGNITE__JDBC = GradleModule(
            "JDBC",
            "ignite-jdbc"
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
        private val IGNITE__PARTITION_REPLICATOR = GradleModule(
            "Partition Replicator",
            "ignite-partition-replicator"
        )
        private val IGNITE__PLACEMENT_DRIVER = GradleModule(
            "Placement Driver",
            "ignite-placement-driver"
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
        private val IGNITE__REPLICATOR = GradleModule(
            "Replicator",
            "ignite-replicator"
        )
        private val IGNITE__RUNNER = GradleModule(
            "Runner",
            "ignite-runner",
            "-XX:MaxDirectMemorySize=256m"
        )
        private val IGNITE__SECURITY = GradleModule(
            "Security",
            "ignite-security"
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
        private val IGNITE__SYSTEM_DISASTER_RECOVERY = GradleModule(
            "System Disaster Recovery",
            "ignite-system-disaster-recovery"
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
        private val IGNITE__COMPATIBILITY = GradleModule(
            "Compatibility",
            "ignite-compatibility-tests",
            dependencies = listOf(":ignite-compatibility-tests:resolveCompatibilityTestDependencies")
        )
        private val MIGRATION_TOOLS_CLI = GradleModule(
            "Migration Tools - CLI",
            "migration-tools-cli"
        )
        private val MIGRATION_TOOLS_COMMONS = GradleModule(
            "Migration Tools - Commons",
            "migration-tools-commons"
        )
        private val MIGRATION_TOOLS_CONFIG_CONVERTER = GradleModule(
            "Migration Tools - Configuration Converter",
            "migration-tools-config-converter"
        )
        private val MIGRATION_TOOLS_PERSISTENCE = GradleModule(
            "Migration Tools - Persistence",
            "migration-tools-persistence")
        private val MIGRATION_TOOLS_E2E_AI3 = GradleModule(
            "Migration Tools - E2E AI3",
            "migration-tools-e2e-ai3-tests"
        )
        private val MIGRATION_TOOLS_ADAPTER = GradleModule(
            "Migration Tools - Adapter",
            "migration-tools-adapter"
        )
        private val MIGRATION_TOOLS_ADAPTER_COMPUTE_CORE = GradleModule(
            "Migration Tools - Compute Core",
            "migration-tools-adapter-compute-core"
        )
        private val MIGRATION_TOOLS_ADAPTER_COMPUTE_BOOTSTRAP = GradleModule(
            "Migration Tools - Compute Bootstrap",
            "migration-tools-adapter-compute-bootstrap"
        )
        private val MIGRATION_TOOLS_ADAPTER_SPRING_TESTS = GradleModule(
            "Migration Tools - Spring Data Tests",
            "migration-tools-adapter-tests-ext-spring-data"
        )
        private val MIGRATION_TOOLS_E2E_AI2 = GradleModule(
            "Migration Tools - E2E AI2",
            "migration-tools-e2e-adapter-tests"
        )
        private val IGNITE__SPRING_BOOT_IGNITE_CLIENT_AUTOCONFIGURE = GradleModule(
            "Spring Boot Client Autoconfigure",
            "spring-boot-ignite-client-autoconfigure"
        )

        /**
         * List of GradleModules with Unit tests
         */
        private val unitTestGradleModuleList: List<GradleModule> = listOf(
            IGNITE__API,
            IGNITE__ARCH_TEST,
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
            IGNITE__PLACEMENT_DRIVER,
            IGNITE__RAFT,
            IGNITE__REPLICATOR,
            IGNITE__REST,
            IGNITE__REST_API,
            IGNITE__RUNNER,
            IGNITE__SCHEMA,
            IGNITE__SECURITY,
            IGNITE__SQL_ENGINE,
            IGNITE__STORAGE_API,
            IGNITE__STORAGE_PAGE_MEMORY,
            IGNITE__STORAGE_ROCKSDB,
            IGNITE__SYSTEM_DISASTER_RECOVERY,
            IGNITE__TABLE,
            IGNITE__TRANSACTIONS,
            IGNITE__VAULT,
            IGNITE__SPRING_BOOT_IGNITE_CLIENT_AUTOCONFIGURE
        )

        private val integrationTestGradleModulesList: List<GradleModule> = listOf(
            IGNITE__CLI,
            IGNITE__CLIENT_HANDLER,
            IGNITE__CLUSTER_MANAGEMENT,
            IGNITE__CODE_DEPLOYMENT,
            IGNITE__COMPATIBILITY,
            IGNITE__COMPUTE,
            IGNITE__CONFIGURATION_ANNOTATION_PROCESSOR,
            IGNITE__DISTRIBUTION_ZONES,
            IGNITE__EXAMPLES,
            IGNITE__FILE_TRANSFER,
            IGNITE__INDEX,
            IGNITE__JDBC,
            IGNITE__METASTORAGE,
            IGNITE__METRICS,
            IGNITE__NETWORK,
            IGNITE__PAGE_MEMORY,
            IGNITE__PARTITION_REPLICATOR,
            IGNITE__PLACEMENT_DRIVER,
            IGNITE__RAFT,
            IGNITE__REPLICATOR,
            IGNITE__REST,
            IGNITE__RUNNER,
            IGNITE__SECURITY,
            IGNITE__SQL_ENGINE,
            IGNITE__SYSTEM_DISASTER_RECOVERY,
            IGNITE__TABLE,
            IGNITE__TRANSACTIONS,
        )

        private val migrationToolsIntegrationModules: List<GradleModule> = listOf(
            MIGRATION_TOOLS_CONFIG_CONVERTER,
            MIGRATION_TOOLS_PERSISTENCE,
            MIGRATION_TOOLS_E2E_AI3,
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
            integrationTestGradleModulesList,
            excludeOnlyModules = migrationToolsIntegrationModules,
        )

        private val sqlConfigurationLogic = TestConfiguration(
            "Sql Integration Logic",
            "integrationTest --tests org.apache.ignite.internal.sql.sqllogic.ItSqlLogicTest",
            4,
            60,
            "-DsqlTest")
        private val sqlConfigurationLogic2 = TestConfiguration(
            "Sql Integration Logic2",
            "integrationTest --tests org.apache.ignite.internal.sql.sqllogic.ItSqlLogic2Test",
            4,
            60,
            "-DsqlTest")
        private val sqlConfigurationLogic3 = TestConfiguration(
            "Sql Integration Logic3",
            "integrationTest --tests org.apache.ignite.internal.sql.sqllogic.ItSqlLogic3Test",
            4,
            60,
            "-DsqlTest")

        val SQL_LOGIC: List<Tests> = listOf(
            Tests(sqlConfigurationLogic, listOf(IGNITE__SQL_ENGINE),false),
            Tests(sqlConfigurationLogic2, listOf(IGNITE__SQL_ENGINE),false),
            Tests(sqlConfigurationLogic3, listOf(IGNITE__SQL_ENGINE),false),
        )

        val MIGRATION_TOOLS_INTEGRATION: Tests = Tests(
            TestConfiguration("Integration", "integrationTest", 4, 60, dindSupport = true),
            migrationToolsIntegrationModules,
            enableOthers = false
        )

        val MIGRATION_TOOLS_SUITE: RunTests = RunTests(MIGRATION_TOOLS_INTEGRATION, "Migration Tools Integration")
    }
}
