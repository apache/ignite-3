package test.template_types

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.CustomFailureConditions.Companion.failOnExactText
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


class TestsModule(
    private val configuration: TestConfiguration,
    private val module: GradleModule
): BuildType({
    id(getId(this::class, "${configuration.suiteId} Tests_${module.displayName}", true))
    name = configuration.suiteId + " " + module.displayName

    artifactRules = """
        ignite-3/modules/${module.moduleName}/build/reports/**/index.html
    """.trimIndent()

    params {
        hiddenText("XMX", configuration.xmx.toString() + "g")
        hiddenText("JVM_ARGS", module.jvmArgs)
    }

    steps {
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
        customGradle {
            name = "Run tests"
            tasks = module.buildTask(configuration.testTask)
            workingDir = "%VCSROOT__IGNITE3%"
            jvmArgs = """
                -Xmx%XMX%
                %JVM_ARGS%
            """.trimIndent()
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
    }

    failureConditions {
        executionTimeoutMin = configuration.executionTimeoutMin
    }

    requirements {
        if (configuration.dindSupport) equals("env.DIND_ENABLED", "true") else {}
    }
})
