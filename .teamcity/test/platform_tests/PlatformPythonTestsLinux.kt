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
        param("TOX_ENV", "py310")
        param("PYTHON_VERSION", "3.10")
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
                
                pyenv install %PYTHON_VERSION% || echo 'third'
                pyenv shell %PYTHON_VERSION% || exit 1

                pyenv exec python -m venv .venv_tox || exit 2
                . .venv_tox/bin/activate || exit 3
                
                pyenv exec python -m pip install --upgrade pip || exit 4
                pyenv exec python -m pip install --upgrade tox || exit 5
                pyenv rehash || exit 6

                pyenv exec tox -e %TOX_ENV% || exit 7
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

    requirements {
        equals("env.DIND_ENABLED", "true")
    }
})
