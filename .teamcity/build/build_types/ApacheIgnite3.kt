package build.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.BuildStep
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object ApacheIgnite3 : BuildType({
    id(getId(this::class))
    name = "Apache Ignite 3"

    steps {
        customScript(type = "bash") {
            name = "Clean up local maven repository"
        }

        customGradle {
            name = "Build Apache Ignite 3"
            tasks = "assemble integrationTestClasses testClasses"
            executionMode = BuildStep.ExecutionMode.RUN_ON_FAILURE
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
