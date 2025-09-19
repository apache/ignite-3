package test.integration_tests

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.MIGRATION_TOOLS_INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.SQL_LOGIC
import org.apache.ignite.teamcity.Teamcity.Companion.getId

object Project : Project({
    id(getId(this::class))
    name = "[Integration Tests]"

    INTEGRATION.buildType().forEach { buildType(it) }
    MIGRATION_TOOLS_INTEGRATION.buildType().forEach { buildType(it) }
    SQL_LOGIC.forEach { tests -> tests.buildType().forEach { buildType(it) } }
})
