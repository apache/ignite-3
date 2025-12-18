package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


object CodeStyleJava17 : BuildType({
    id(getId(this::class))
    name = "Code Style (Java 17)"
    description = "Check code's style rules with Checkstyle on Java 17"

    artifactRules = """
        %VCSROOT__IGNITE3%/build/reports/checkstyle/*.html
    """.trimIndent()

    params {
        hiddenText("env.JAVA_HOME", "%env.JDK_ORA_17%")
    }

    steps {
        customGradle {
            name = "Check code style by Checkstyle Gradle Plugin"
            tasks = "checkstyle"
            gradleParams = "--continue"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }

    failureConditions {
        executionTimeoutMin = 10
        failOnText {
            conditionType = BuildFailureOnText.ConditionType.REGEXP
            pattern = "There.*[0-9]+ error(s)? reported by Checkstyle"
            failureMessage = "CheckStyle errors"
            reverse = false
        }
    }
})
