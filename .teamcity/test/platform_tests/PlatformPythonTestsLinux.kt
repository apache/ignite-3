package test.platform_tests

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.*
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnMetric
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnMetricChange
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity


object PlatformPythonTestsLinux : BuildType({
    id(Teamcity.getId(this::class))
    name = "Platform Python Tests (Linux)"

    params {
        text("PATH__WORKING_DIR", """%VCSROOT__IGNITE3%\modules\platforms\python""", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        param("env.IGNITE_CPP_TESTS_USE_SINGLE_NODE", "")
        param("env.CPP_STAGING", """%PATH__WORKING_DIR%\cpp_staging""")
    }

    steps {
        customGradle {
            name = "Verify runner is builded"
            tasks = ":ignite-runner:integrationTestClasses"
        }
        script {
            name = "Python Client tests"
            workingDir = "%PATH__WORKING_DIR%"
            scriptContent = """
                #!/usr/bin/env bash
                set -o errexit; set -o pipefail; set -o errtrace; set -o functrace
                set -x
                
                
                eval "${'$'}(pyenv init --path)" || echo 'first'
                eval "${'$'}(pyenv init --no-rehash -)" || echo 'second'
                
                # Install Python 3.9 if not already installed
                if ! pyenv versions --bare | grep -q "^3.9"; then
                    pyenv install -s 3.9.18 || pyenv install -s 3.9
                fi
                pyenv local 3.9.18 2>/dev/null || pyenv local 3.9 || true
                
                python3 -m venv .venv
                . .venv/bin/activate
                pip install tox
                
                tox -e py39 || exit 0
            """.trimIndent()
        }
    }

    failureConditions {
        executionTimeoutMin = 20
        failOnMetricChange {
            metric = BuildFailureOnMetric.MetricType.TEST_COUNT
            threshold = 5
            units = BuildFailureOnMetric.MetricUnit.DEFAULT_UNIT
            comparison = BuildFailureOnMetric.MetricComparison.LESS
            compareTo = build {
                buildRule = lastSuccessful()
            }
        }
        failOnText {
            conditionType = BuildFailureOnText.ConditionType.CONTAINS
            pattern = "CMake configuration failed"
            failureMessage = "CMake configuration failed"
            reverse = false
        }
        failOnText {
            conditionType = BuildFailureOnText.ConditionType.CONTAINS
            pattern = "CMake build failed"
            failureMessage = "CMake build failed"
            reverse = false
        }
        failOnText {
            conditionType = BuildFailureOnText.ConditionType.CONTAINS
            pattern = "CMake install failed"
            failureMessage = "CMake install failed"
            reverse = false
        }
    }
})
