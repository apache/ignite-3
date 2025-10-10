package test.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.MIGRATION_TOOLS_SUITE
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.SQL_LOGIC
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.UNIT
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText
import test.template_types.RunTests
import test.template_types.RunTestsList

object RunAllTestsCustomJDK : BuildType({
    id(getId(this::class))
    name = "--> Run :: All Tests [JDK 11/21]"
    description = "Run all tests on custom JDK at once"
    type = Type.COMPOSITE

    params {
        checkbox("reverse.dep.*.IGNITE_COMPATIBILITY_TEST_ALL_VERSIONS", "-DtestAllVersions=false", label = "Test All Versions", description = "Test all versions in compatibility tests", checked = "-DtestAllVersions=true", unchecked = "-DtestAllVersions=false")
        checkbox("reverse.dep.*.IGNITE_ZONE_BASED_REPLICATION", "-DIGNITE_ZONE_BASED_REPLICATION=false", label = "Disable Zone-based replication", checked = "-DIGNITE_ZONE_BASED_REPLICATION=true", unchecked = "-DIGNITE_ZONE_BASED_REPLICATION=false")
        select("reverse.dep.*.IGNITE_DEFAULT_STORAGE_ENGINE", "", label = "Default Storage Engine", options = listOf("Default" to "", "aimem" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=aimem", "rocksdb" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=rocksdb"))
    }

    dependencies {
        snapshot(RunSanityCheck) {}
        snapshot(RunPlatformTests) {}
        snapshot(RunTests(INTEGRATION)) {}
        snapshot(MIGRATION_TOOLS_SUITE) {}
        snapshot(RunTests(UNIT)) {}
        snapshot(RunTestsList(SQL_LOGIC, SQL_LOGIC[0].configuration.suiteId)) {}
    }
})
