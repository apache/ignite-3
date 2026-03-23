package org.apache.ignite.teamcity

import jetbrains.buildServer.configs.kotlin.BuildSteps
import jetbrains.buildServer.configs.kotlin.buildSteps.*
import java.io.File


@Suppress("unused")
class CustomBuildSteps {
    companion object {

        fun BuildSteps.customGradle(init: GradleBuildStep.() -> Unit): GradleBuildStep {
            val result = GradleBuildStep(init)

            // Use current %JAVA_HOME% as default JDK for compiling using Gradle
            result.jdkHome = if (result.jdkHome == null) "%env.JAVA_HOME%" else result.jdkHome
            result.gradleParams = if (result.gradleParams.isNullOrBlank())
                "-Dorg.gradle.caching=true" else result.gradleParams.plus(" -Dorg.gradle.caching=true")
            result.useGradleWrapper = true
            result.workingDir = "%VCSROOT__IGNITE3%"

            step(result)
            return result
        }


        /**
         * Custom POWERSHELL build step
         *
         * @param filename: name of script file (without path)
         */
        fun BuildSteps.customPowerShell(
            filename: String = "",
            init: PowerShellStep.() -> Unit
        ): PowerShellStep {
            val result = PowerShellStep(init)
            val file = if (filename == "") Teamcity.getBashScriptFileName(result.name) else filename

            result.platform = PowerShellStep.Platform.x64
            result.edition = PowerShellStep.Edition.Desktop
            result.noProfile = false

            val scriptModeContent = PowerShellStep.ScriptMode.Script()
            scriptModeContent.content = File("files/scripts/powershell/${file}.ps1").readText()
            result.scriptMode = scriptModeContent

            step(result)
            return result
        }


        /**
         * Custom COMMAND LINE (bash/cmd script) step
         *
         * @param type: script type (bash/cmd)
         * @param filename: name of script file (without path)
         * @param replacements: ?
         */
        fun BuildSteps.customScript(
            type: String,
            filename: String = "",
            replacements: Map<String, String>? = null,
            init: ScriptBuildStep.() -> Unit
        ): ScriptBuildStep {
            val result = ScriptBuildStep(init)
            // If file name is not specified, use script step name as script name accordingly converted
            val file = if (filename == "") Teamcity.getBashScriptFileName(result.name) else filename
            var extension = ""

            when (type) {
                "bash" -> {
                    result.scriptContent = """
                        #!/usr/bin/env bash
                        set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace
                        set -x
                """.trimIndent()
                    extension = "sh"
                }

                "cmd" -> {
                    result.scriptContent = "@ECHO ON"
                    extension = "cmd"
                }
            }

            result.scriptContent += "\n\n\n" + File("files/scripts/${type}/${file}.${extension}").readText()
            replacements?.forEach { (k, v) -> result.scriptContent = result.scriptContent?.replace(k, v) }

            step(result)
            return result
        }


        /**
         * Custom PYTHON build step
         *
         * @param filename: name of script file (without path)
         * @param scriptArgs: arguments for running python script
         */
        fun BuildSteps.customPython(
                filename: String = "",
                scriptArgs: String = "",
                init: PythonBuildStep.() -> Unit
        ): PythonBuildStep {
            val result = PythonBuildStep(init)
            val file = if (filename == "") Teamcity.getBashScriptFileName(result.name) else filename

            val scriptContent = PythonBuildStep.Command.Script()
            scriptContent.content = File("files/scripts/python/${file}.py").readText()
            scriptContent.scriptArguments = scriptArgs
            result.command = scriptContent

            step(result)
            return result
        }
    }
}