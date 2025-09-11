package test.build_types

import _Self.isActiveProject
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import org.apache.ignite.teamcity.CustomTriggers.Companion.customSchedule
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.MIGRATION_TOOLS_SUITE
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.SQL_LOGIC
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.UNIT
import org.apache.ignite.teamcity.CustomTriggers.Companion.integrationBranchChange
import org.apache.ignite.teamcity.CustomTriggers.Companion.pullRequestChange
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.template_types.RunTests
import test.template_types.RunTestsList

object RunAllTests : BuildType({
    id(getId(this::class))
    name = "-> Run :: All Tests"
    description = "Run all tests at once"
    type = Type.COMPOSITE

    triggers {
        customSchedule(5, "+:<default>", enabled = isActiveProject) {}
        pullRequestChange(enabled = isActiveProject) {}
        integrationBranchChange(enabled = isActiveProject) {}
    }

    params {
        checkbox("reverse.dep.*.IGNITE_ZONE_BASED_REPLICATION", "false", label = "Disable Zone-based replication", checked = "true", unchecked = "false")
        select("reverse.dep.*.env.A_GRADLE_OPTS", "", label = "Default Storage Engine", options = listOf("Default" to "", "aimem" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=aimem", "rocksdb" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=rocksdb"))
    }

    dependencies {
        snapshot(RunSanityCheck) {}
        snapshot(RunTests(INTEGRATION)) {}
        snapshot(MIGRATION_TOOLS_SUITE) {}
        snapshot(RunTests(UNIT)) {}
        snapshot(RunTestsList(SQL_LOGIC, SQL_LOGIC[0].configuration.suiteId)) {}
    }
})
