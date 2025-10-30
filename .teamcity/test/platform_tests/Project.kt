package test.platform_tests

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object Project : Project({
    id(getId(this::class))
    name = "[Platform Tests]"

    /**
     * List of platform linux tests
     */

    listOf(
        PlatformCppTestsLinux,
        PlatformDotnetTestsLinux,
        PlatformPythonTestsLinux
    ).forEach {
        buildType(
            ApacheIgnite3CustomBuildType.Builder(it)
                .ignite3VCS().ignite3BuildDependency()
                .defaultBuildTypeSettings().requireLinux()
                .build().buildType
        )
    }

    /**
     * List of platform windows tests
     */

    listOf(
        PlatformCppTestsWindows,
        PlatformDotnetTestsWindows
    ).forEach {
        buildType(
            ApacheIgnite3CustomBuildType.Builder(it)
                .ignite3VCS().ignite3BuildDependency()
                .defaultBuildTypeSettings().requireWindows()
                .build().buildType
        )
    }
})
