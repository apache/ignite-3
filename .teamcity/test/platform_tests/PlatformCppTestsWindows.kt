package test.platform_tests

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildFeatures.XmlReport
import jetbrains.buildServer.configs.kotlin.buildFeatures.xmlReport
import jetbrains.buildServer.configs.kotlin.buildSteps.*
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnMetric
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnMetricChange
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import jetbrains.buildServer.configs.kotlin.triggers.vcs
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.Teamcity
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


object PlatformCppTestsWindows : BuildType({
    id(Teamcity.getId(this::class))
    name = "Platform C++ Tests (Windows)"

    artifactRules = """
        %PATH__UNIT_TESTS_RESULT% => test_logs
        %PATH__CLIENT_TEST_RESULTS% => test_logs
    """.trimIndent()

    params {
        hiddenText("env.IGNITE_CPP_TESTS_USE_SINGLE_NODE", "")
        hiddenText("PATH__CMAKE_BUILD_DIRECTORY", """%PATH__WORKING_DIR%\cmake-build-debug""")
        hiddenText("PATH__CLIENT_TEST_RESULTS", """%PATH__CMAKE_BUILD_DIRECTORY%\cpp_client_tests_results.xml""")
        hiddenText("PATH__ODBC_TEST_RESULTS", """%PATH__CMAKE_BUILD_DIRECTORY%\odbc_tests_results.xml""")
        hiddenText("PATH__UNIT_TESTS_RESULT", """%PATH__CMAKE_BUILD_DIRECTORY%\cpp_unit_test_results.xml""")
        hiddenText("PATH__WORKING_DIR", """%VCSROOT__IGNITE3%\modules\platforms\cpp""")
        hiddenText("env.CPP_STAGING", """%PATH__WORKING_DIR%\cpp_staging""")
    }

    steps {
        script {
            name = "Build C++"
            scriptContent = """
                @echo on
                
                mkdir %PATH__CMAKE_BUILD_DIRECTORY%
                cd %PATH__CMAKE_BUILD_DIRECTORY%
                
                cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DWARNINGS_AS_ERRORS=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=%env.CPP_STAGING% -DCMAKE_CONFIGURATION_TYPES="Debug" -G "Visual Studio 15 2017" -A x64
                
                @echo off
                if %%ERRORLEVEL%% NEQ 0 (
                  echo 'CMake configuration failed'
                  exit 5
                )
                @echo on
                
                cmake --build . -j8
                
                @echo off
                if %%ERRORLEVEL%% NEQ 0 (
                  echo 'CMake build failed'
                  exit 6
                )
                @echo on
                
                cmake --install .
                
                @echo off
                if %%ERRORLEVEL%% NEQ 0 (
                  echo 'CMake install failed'
                  exit 7
                )
                @echo on
            """.trimIndent()
        }
        script {
            name = "Unit tests"
            workingDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
            scriptContent = """
                ctest --version
                ctest -E "(IgniteClientTest|IgniteOdbcTest)" --output-junit %PATH__UNIT_TESTS_RESULT%
                exit 0
            """.trimIndent()
            formatStderrAsError = true
        }
        customGradle {
            name = "Verify runner is builded"
            tasks = ":ignite-runner:integrationTestClasses"
        }
        script {
            name = "C++ Client integration tests"
            workingDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
            scriptContent = """Debug\bin\ignite-client-test --gtest_output=xml:%PATH__CLIENT_TEST_RESULTS%"""
            formatStderrAsError = true
        }
        script {
            name = "ODBC integration tests"
            enabled = false
            workingDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
            scriptContent = """Debug\bin\ignite-odbc-test --gtest_output=xml:%PATH__ODBC_TEST_RESULTS%"""
            formatStderrAsError = true
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

    features {
        xmlReport {
            reportType = XmlReport.XmlReportType.GOOGLE_TEST
            rules = """
                +:%PATH__CLIENT_TEST_RESULTS%
                +:%PATH__ODBC_TEST_RESULTS%
                +:%PATH__CMAKE_BUILD_DIRECTORY%/Testing/Result/*.xml
            """.trimIndent()
            verbose = true
        }
        xmlReport {
            reportType = XmlReport.XmlReportType.CTEST
            rules = "+:%PATH__UNIT_TESTS_RESULT%"
            verbose = true
        }
    }
})
