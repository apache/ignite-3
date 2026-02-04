package test.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.platform_tests.*

object RunPlatformTests : BuildType({
    id(getId(this::class))
    name = "> Run :: Platform Tests"
    description = "Run all platform tests at once"
    type = Type.COMPOSITE

    dependencies {
        snapshot(PlatformCppTestsLinux) {}
        snapshot(PlatformCppOdbcTestsLinux) {}
//        snapshot(PlatformCppTestsWindows) {}  // Always falling, under investigation
        snapshot(PlatformDotnetTestsWindows) {}
        snapshot(PlatformDotnetTestsLinux) {}
        snapshot(PlatformPythonTestsLinux) {}
    }
})
