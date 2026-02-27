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
        PlatformCppOdbcTestsDebLinux,
        PlatformCppOdbcTestsRpmLinux,
        PlatformCppOdbcTestsTgzLinux,
        PlatformDotnetTestsLinux,
        RunPythonTests
    ).forEach {
        buildType(
            ApacheIgnite3CustomBuildType.Builder(it)
                .ignite3VCS().ignite3BuildDependency().setupMavenProxy()
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
