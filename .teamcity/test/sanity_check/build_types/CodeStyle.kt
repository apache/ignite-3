package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object CodeStyle : BuildType({
    id(getId(this::class))
    name = "Code Style"
    description = "Check code's style rules with Checkstyle"

    artifactRules = """
        %VCSROOT__IGNITE3%/build/reports/checkstyle/*.html
    """.trimIndent()

    steps {
        customGradle {
            name = "Check code style by Checkstyle Gradle Plugin"
            tasks = "checkstyleMain checkstyleTest checkstyleIntegrationTest checkstyleTestFixtures"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
