package test.platform_tests

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.*
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.Teamcity


object PlatformDotnetTestsLinux : BuildType({
    id(Teamcity.getId(this::class))
    name = "Platform .NET Tests (Linux)"

    artifactRules = """
        **/hs_err*.log => crashdumps.zip
        **/*.hprof
    """.trimIndent()

    params {
        text("PATH__WORKING_DIR", "%VCSROOT__IGNITE3%/modules/platforms/dotnet/", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        param("env.IGNITE_DOTNET_GRADLE_OPTS", "--no-daemon")
    }

    steps {
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
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
        dotnetPack {
            name = ".NET Pack (validate)"
            workingDir = "%PATH__WORKING_DIR%"
            configuration = "Release"
            args = "--no-restore -m:8"
        }
        customGradle {
            name = "Build Java Node Runner"
            tasks = ":ignite-runner:integrationTestClasses :ignite-compatibility-tests:testFixturesClasses"
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
        dotnetTest {
            name = ".NET Test (Debug)"
            workingDir = "%PATH__WORKING_DIR%"
            configuration = "Debug"
            skipBuild = true
            args = "-p:BuildInParallel=false -m:1"
            sdk = "3.1 6"
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
        dotnetTest {
            name = ".NET Test (Release)"
            workingDir = "%PATH__WORKING_DIR%"
            configuration = "Release"
            skipBuild = true
            args = "-p:BuildInParallel=false -m:1"
            sdk = "3.1 6"
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
        script {
            name = "Build AOT Tests"
            workingDir = "%PATH__WORKING_DIR%"
            scriptContent = "dotnet publish --runtime linux-x64 --configuration Release Apache.Ignite.Tests.Aot"
        }
        script {
            name = "Run AOT Tests"
            workingDir = "%PATH__WORKING_DIR%"
            scriptContent = "./Apache.Ignite.Tests.Aot/bin/Release/net8.0/linux-x64/publish/Apache.Ignite.Tests.Aot"
        }
    }

    failureConditions {
        failOnText {
            conditionType = BuildFailureOnText.ConditionType.CONTAINS
            pattern = "NullReferenceException"
            failureMessage = "NullReferenceException in log"
            reverse = false
        }
    }

    requirements {
        equals("env.DIND_ENABLED", "true")
    }
})
