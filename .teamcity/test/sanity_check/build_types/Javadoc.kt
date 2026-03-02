package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.CustomFailureConditions.Companion.failOnExactText
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object Javadoc : BuildType({
    id(getId(this::class))
    name = "Javadoc"
    description = "Check Javadoc correctness and style"

    artifactRules = """
        target/site/apidocs => javadoc.zip
        target/checkstyle.xml
        target/site/checkstyle-aggregate.html
    """.trimIndent()

    steps {
        customGradle {
            name = "Build Javadoc JDK 11"
            tasks = "aggregateJavadoc"
            workingDir = "%VCSROOT__IGNITE3%"
            jdkHome = "%env.JDK_ORA_11%"
        }

        customGradle {
            name = "Build Javadoc"
            tasks = "aggregateJavadoc"
            workingDir = "%VCSROOT__IGNITE3%"
        }

        customScript(type = "bash") {
            name = "Check internal packages"
        }
    }

    failureConditions {
        failOnExactText(pattern = "[ERROR] Internal packages detected", failureMessage = "[ERROR] Internal packages detected") {}
    }
})
