package test.template_types

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


class OtherTestsModule(
    private val configuration: TestConfiguration,
    private val excludeModules: List<GradleModule>,
    private val jvmArgs: String = ""
): BuildType({
    id(getId(this::class, "${configuration.suiteId} All_Other_Tests", true))
    name = configuration.suiteId + " All_Other_Tests"

    artifactRules = """
        %VCSROOT__IGNITE3%/build/reports/**/index.html
    """.trimIndent()

    params {
        hiddenText("XMX", configuration.xmx.toString() + "g")
        hiddenText("JVM_ARGS", jvmArgs + configuration.jvmArg)

        hiddenText("IGNITE_COMPATIBILITY_TEST_ALL_VERSIONS", "-DtestAllVersions=false")
        hiddenText("IGNITE_ZONE_BASED_REPLICATION", "-DIGNITE_ZONE_BASED_REPLICATION=true")
        hiddenText("IGNITE_DEFAULT_STORAGE_ENGINE", "")
        hiddenText("env.GRADLE_OPTS", "-PextraJvmArgs=\"%IGNITE_COMPATIBILITY_TEST_ALL_VERSIONS% %IGNITE_ZONE_BASED_REPLICATION% %IGNITE_DEFAULT_STORAGE_ENGINE%\"")
    }

    steps {
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }

        customGradle {
            id = "RunTests"
            name = "Run tests"
            tasks = configuration.testTask + " " +
                excludeModules.map { "-x " + it.buildTask(configuration.testTask) }.joinToString(" ")
            workingDir = "%VCSROOT__IGNITE3%"
            gradleParams = "%env.GRADLE_OPTS%"
            this.jvmArgs = """
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
