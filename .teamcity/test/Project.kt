package test

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.INTEGRATION
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.MIGRATION_TOOLS_SUITE
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.SQL_LOGIC
import org.apache.ignite.teamcity.ApacheIgnite3Teamcity.Companion.UNIT
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.build_types.RunAllTests
import test.build_types.RunAllTestsCustomJDK
import test.build_types.RunPlatformTests
import test.build_types.RunSanityCheck
import test.platform_tests.PlatformDotnetTestsLinux
import test.template_types.RunTests
import test.template_types.RunTestsList


object Project : Project({
    id(getId(this::class))
    name = "[Test]"

    subProject(test.integration_tests.Project)
    subProject(test.sanity_check.Project)
    subProject(test.unit_tests.Project)
    subProject(test.platform_tests.Project)

    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunAllTests)
            .ignite3VCS().ignite3CommitStatusPublisher()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunAllTestsCustomJDK)
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunSanityCheck)
            .ignite3VCS().ignite3CommitStatusPublisher()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunPlatformTests)
            .ignite3VCS().ignite3CommitStatusPublisher()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunTests(INTEGRATION))
            .ignite3VCS().ignite3CommitStatusPublisher()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(MIGRATION_TOOLS_SUITE)
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunTests(UNIT))
            .ignite3VCS().ignite3CommitStatusPublisher()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
    buildType(
        ApacheIgnite3CustomBuildType.Builder(RunTestsList(SQL_LOGIC, SQL_LOGIC[0].configuration.suiteId))
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
})

