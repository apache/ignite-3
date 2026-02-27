package test.platform_tests

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId

object RunPythonTests : BuildType({
    id(getId(this::class))
    name = "-> Run :: Python Tests"
    description = "Run all Python Tests"
    type = Type.COMPOSITE

    dependencies {
        test.platform_tests.python_tests.Project.buildTypes.forEach{
            snapshot(it) {}
        }
    }
})
