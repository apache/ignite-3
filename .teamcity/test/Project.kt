package test

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.UNIT
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.build_types.RunAllTests
import test.build_types.RunSanityCheck
import test.template_types.RunTests


object Project : Project({
    id(getId(this::class))
    name = "[Test]"

    subProject(test.integration_tests.Project)
    subProject(test.sanity_check.Project)
    subProject(test.unit_tests.Project)

    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunAllTests)
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunSanityCheck)
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunTests(INTEGRATION))
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunTests(UNIT))
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
})

