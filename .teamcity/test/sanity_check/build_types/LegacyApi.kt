package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity

object LegacyApi: BuildType({
    id(Teamcity.getId(this::class))
    name = "LegacyApi"
    description = "Check code for Legacy API with Modernizer"

    steps {
        customGradle {
            name = "Check for Legacy API by Modernizer Gradle plugin"
            tasks = "modernizer"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})