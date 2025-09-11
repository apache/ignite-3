package test.platform_tests

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object Project : Project({
    id(getId(this::class))
    name = "[Platform Tests]"

    buildType(
        ApacheIgnite3CustomBuildType.Builder(PlatformDotnetTestsLinux)
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )

    buildType(
        ApacheIgnite3CustomBuildType.Builder(PlatformDotnetTestsWindows)
            .ignite3VCS()
            .defaultBuildTypeSettings().requireWindows()
            .build().buildType
    )
})
