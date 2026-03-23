package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity

object Spotbugs: BuildType({
    id(Teamcity.getId(this::class))
    name = "Spotbugs"
    description = "Static analysis of source code via Spotbugs"

    steps {
        customGradle {
            name = "Static analysis of source code via Spotbugs"
            tasks = "spotbugsMain"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})