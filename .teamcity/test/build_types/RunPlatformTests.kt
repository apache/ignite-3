package test.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.platform_tests.*

object RunPlatformTests : BuildType({
    id(getId(this::class))
    name = "> Run :: Platform Tests"
    description = "Run all platform tests at once"
    type = Type.COMPOSITE

    params {
        checkbox("reverse.dep.*.IGNITE_ZONE_BASED_REPLICATION", "false", label = "Disable Zone-based replication", checked = "true", unchecked = "false")
        select("reverse.dep.*.env.A_GRADLE_OPTS", "", label = "Default Storage Engine", options = listOf("Default" to "", "aimem" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=aimem", "rocksdb" to "-DIGNITE_DEFAULT_STORAGE_ENGINE=rocksdb"))
    }

    dependencies {
        snapshot(PlatformCppTestsLinux) {}
        snapshot(PlatformCppTestsWindows) {}
        snapshot(PlatformDotnetTestsWindows) {}
        snapshot(PlatformDotnetTestsLinux) {}
        snapshot(PlatformPythonTestsLinux) {}
    }
})
