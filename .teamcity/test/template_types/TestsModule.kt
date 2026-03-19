package test.template_types

import jetbrains.buildServer.configs.kotlin.BuildStep
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildFeatures.parallelTests
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


class TestsModule(
    private val configuration: TestConfiguration,
    private val module: GradleModule
) : BuildType({
    id(getId(this::class, "${configuration.suiteId} Tests_${module.displayName}", true))
    name = configuration.suiteId + " " + module.displayName

    artifactRules = """
        ignite-3/modules/${module.moduleName}/build/reports/**/index.html
    """.trimIndent()

    params {
        hiddenText("XMX", configuration.xmx.toString() + "g")
        hiddenText("JVM_ARGS", module.jvmArgs + configuration.jvmArg)

        hiddenText("IGNITE_COMPATIBILITY_TEST_ALL_VERSIONS", "-DtestAllVersions=false")
        hiddenText("IGNITE_DEFAULT_STORAGE_ENGINE", "")
        hiddenText("EXTRA_GRADLE_OPTS", "-PextraJvmArgs=\"%IGNITE_COMPATIBILITY_TEST_ALL_VERSIONS% %IGNITE_DEFAULT_STORAGE_ENGINE%\"")
    }

    steps {
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }

        customScript(type = "bash") {
            name = "Setup Docker Proxy"
        }

        customGradle {
            name = "Run tests"
            tasks = module.buildTask(configuration.testTask)
            workingDir = "%VCSROOT__IGNITE3%"
            gradleParams = "%env.GRADLE_OPTS% %EXTRA_GRADLE_OPTS%"
            jvmArgs = """
                -Xmx%XMX%
                %JVM_ARGS%
            """.trimIndent()
        }

        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }

        customScript(type = "bash") {
            id = "PruneDockerImages"
            name = "DockerImagePrune"
            executionMode = BuildStep.ExecutionMode.ALWAYS
            conditions {
                equals("env.DIND_ENABLED", "true")
            }
        }
    }

    features {
        parallelTests {
            enabled = module.parallelTestsEnabled
            numberOfBatches = 15
        }
    }

    failureConditions {
        executionTimeoutMin = configuration.executionTimeoutMin
    }

    requirements {
        if (configuration.dindSupport) equals("env.DIND_ENABLED", "true") else {}
    }
})
