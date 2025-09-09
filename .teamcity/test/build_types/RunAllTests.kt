package test.build_types

import _Self.isActiveProject
import jetbrains.buildServer.configs.kotlin.triggers.vcs
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.DslContext
import org.apache.ignite.teamcity.CustomTriggers.Companion.customSchedule
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.UNIT
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.template_types.RunTests

object RunAllTests : BuildType({
    id(getId(this::class))
    name = "--> Run :: All Tests"
    description = "Run all tests at once"
    type = Type.COMPOSITE

    triggers {
        customSchedule(5, "+:<default>", enabled = isActiveProject) {}
    }

    dependencies {
        snapshot(RunSanityCheck) {}
        snapshot(RunTests(INTEGRATION)) {}
        snapshot(RunTests(UNIT)) {}
    }
})
