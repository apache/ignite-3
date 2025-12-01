package test.platform_tests

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildFeatures.Swabra
import jetbrains.buildServer.configs.kotlin.buildFeatures.swabra
import jetbrains.buildServer.configs.kotlin.buildSteps.*
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity


object PlatformDotnetTestsWindows : BuildType({
    id(Teamcity.getId(this::class))
    name = "Platform .NET Tests (Windows)"

    artifactRules = """
        **/hs_err*.log => crashdumps.zip
        **/*.hprof
    """.trimIndent()

    params {
        text("PATH__WORKING_DIR", "%VCSROOT__IGNITE3%/modules/platforms/dotnet/", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        param("env.IGNITE_DOTNET_GRADLE_OPTS", "--no-daemon")
    }

    steps {
        script {
            name = ".NET List SDKs"
            scriptContent = "dotnet --list-sdks"
        }
        dotnetRestore {
            name = ".NET Restore"
            workingDir = "%PATH__WORKING_DIR%"
        }
        dotnetBuild {
            name = ".NET Build (Debug)"
            workingDir = "%PATH__WORKING_DIR%"
            configuration = "Debug"
            args = "--no-restore -m:1"
        }
        dotnetBuild {
            name = ".NET Build (Release)"
            workingDir = "%PATH__WORKING_DIR%"
            configuration = "Release"
            args = "--no-restore -m:1"
        }
        customGradle {
            name = "Verify runner is built"
            tasks = ":ignite-runner:integrationTestClasses :ignite-compatibility-tests:testFixturesClasses"
        }
        dotnetTest {
            name = ".NET Test (Release)"
            workingDir = "%PATH__WORKING_DIR%"
            configuration = "Release"
            skipBuild = true
            args = "-p:BuildInParallel=false -m:1 --filter TransactionsTests"
            sdk = "3.1 6"
        }
    }

    failureConditions {
        executionTimeoutMin = 40
        failOnText {
            conditionType = BuildFailureOnText.ConditionType.CONTAINS
            pattern = "NullReferenceException"
            failureMessage = "NullReferenceException in log"
            reverse = false
        }
    }

    features {
        swabra {
            filesCleanup = Swabra.FilesCleanup.AFTER_BUILD
            lockingProcesses = Swabra.LockingProcessPolicy.KILL
            paths = """%VCSROOT__IGNITE3%\modules\compatibility-tests"""
        }
    }
})
