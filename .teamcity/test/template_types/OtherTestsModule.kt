package test.template_types

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


class OtherTestsModule(
    private val configuration: TestConfiguration,
    private val excludeModules: List<GradleModule>,
    private val jvmArgs: String = ""
): BuildType({
    id(getId(this::class, "${configuration.suiteId} All_Other_Tests", true))
    name = configuration.suiteId + " All_Other_Tests"

    params {
        hiddenText("XMX", configuration.xmx.toString() + "g")
        hiddenText("JVM_ARGS", jvmArgs)
    }

    steps {
        customGradle {
            id = "RunTests"
            name = "Run tests"
            tasks = configuration.testTask + " " +
                excludeModules.map { "-x " + it.buildTask(configuration.testTask) }.joinToString(" ")
            workingDir = "%VCSROOT__IGNITE3%"
            this.jvmArgs = """
                -Xmx%XMX%
                %JVM_ARGS%
            """.trimIndent()
        }
    }

    artifactRules = """
        %VCSROOT__IGNITE3%/build/reports/**/index.html
    """.trimIndent()

    failureConditions {
        executionTimeoutMin = configuration.executionTimeoutMin
    }
})
