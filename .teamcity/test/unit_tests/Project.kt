package test.unit_tests

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.UNIT
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object Project : Project({
    id(getId(this::class))
    name = "[Unit Tests]"

    UNIT.buildType().forEach { buildType(it) }
})
