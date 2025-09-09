package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.CustomFailureConditions.Companion.failOnExactText
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


object Javadoc : BuildType({
    id(getId(this::class))
    name = "Javadoc"
    description = "Check Javadoc correctness and style"

    params {
        hiddenText("ERROR_TEXT__INTERNAL_PACKAGES", "[ERROR] Internal packages detected")
    }

    artifactRules = """
        **/build/docs/javadoc/** => javadoc.zip
    """.trimIndent()

    steps {
        customGradle {
            name = "Check Javadoc style"
            tasks = "javadoc"
            workingDir = "%VCSROOT__IGNITE3%"
        }

        customScript(type = "bash") {
            name = "Check internal packages"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }

    failureConditions {
        failOnExactText(pattern = "%ERROR_TEXT__INTERNAL_PACKAGES%", failureMessage = "%ERROR_TEXT__INTERNAL_PACKAGES%") {}
    }
})
