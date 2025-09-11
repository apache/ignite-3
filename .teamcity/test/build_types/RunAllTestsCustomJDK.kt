package test.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.MIGRATION_TOOLS_SUITE
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.SQL_LOGIC
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.UNIT
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.template_types.RunTests
import test.template_types.RunTestsList

object RunAllTestsCustomJDK : BuildType({
    id(getId(this::class))
    name = "--> Run :: All Tests [JDK 11/21]"
    description = "Run all tests on custom JDK at once"
    type = Type.COMPOSITE

    params {
        checkbox("reverse.dep.*.IGNITE_ZONE_BASED_REPLICATION", "false", label = "Disable Zone-based replication", checked = "true", unchecked = "false")
        select("reverse.dep.*.env.A_GRADLE_OPTS", "", label = "Default Storage Engine", options = listOf("Default" to "", "aimem" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=aimem", "rocksdb" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=rocksdb"))
        select("reverse.dep.*.env.JAVA_HOME", "%env.JDK_ORA_11%", label = "JDK", options = listOf("JDK 11" to "%env.JDK_ORA_11%", "JDK 21" to "%env.JDK_ORA_21%"))
    }

    dependencies {
        snapshot(RunSanityCheck) {}
        snapshot(RunTests(INTEGRATION)) {}
        snapshot(MIGRATION_TOOLS_SUITE) {}
        snapshot(RunTests(UNIT)) {}
        snapshot(RunTestsList(SQL_LOGIC, SQL_LOGIC[0].configuration.suiteId)) {}
    }
})
