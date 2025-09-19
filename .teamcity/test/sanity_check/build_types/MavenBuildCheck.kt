package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object MavenBuildCheck : BuildType({
    id(getId(this::class))
    name = "Maven Build Check"
    description = "Check Maven build"
    artifactRules = "%FILE_UNUSED_PROPERTIES%"

    params {
        text("FILE_UNUSED_PROPERTIES", "unused-properties.txt", display = ParameterDisplay.HIDDEN, allowEmpty = true)
    }

    steps {
        maven {
            name = "Check Maven build"
            enabled = false
            goals = "clean package"
            runnerArgs = """
                -DskipTests
                -Dmaven.all-checks.skip
            """.trimIndent()
            userSettingsSelection = "local-proxy.xml"
            jdkHome = "%env.JDK_ORA_11%"
        }
    }
})
