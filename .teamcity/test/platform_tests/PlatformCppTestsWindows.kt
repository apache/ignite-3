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
        %PATH__CRASH_DUMPS% => crash_dumps
    """.trimIndent()

    params {
        hiddenText("env.IGNITE_CPP_TESTS_USE_SINGLE_NODE", "")
        hiddenText("PATH__CMAKE_BUILD_DIRECTORY", """%PATH__WORKING_DIR%\cmake-build-debug""")
        hiddenText("PATH__CLIENT_TEST_RESULTS", """%PATH__CMAKE_BUILD_DIRECTORY%\cpp_client_tests_results.xml""")
        hiddenText("PATH__ODBC_TEST_RESULTS", """%PATH__CMAKE_BUILD_DIRECTORY%\odbc_tests_results.xml""")
        hiddenText("PATH__CRASH_DUMPS", """%PATH__CMAKE_BUILD_DIRECTORY%\dumps""")
        hiddenText("PATH__UNIT_TESTS_RESULT", """%PATH__CMAKE_BUILD_DIRECTORY%\cpp_unit_test_results.xml""")
        hiddenText("PATH__WORKING_DIR", """%VCSROOT__IGNITE3%\modules\platforms\cpp""")
        hiddenText("env.CPP_STAGING", """%PATH__WORKING_DIR%\cpp_staging""")
    }

    steps {

        powerShell {
            name = "Build C++"
            platform = PowerShellStep.Platform.x64
            scriptMode = script {
                content = """
                    ${'$'}ErrorActionPreference = "Stop"
    
                    New-Item -ItemType Directory -Force -Path "%PATH__CMAKE_BUILD_DIRECTORY%" | Out-Null
                    Set-Location "%PATH__CMAKE_BUILD_DIRECTORY%"
    
                    cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DWARNINGS_AS_ERRORS=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX="%env.CPP_STAGING%" -DCMAKE_CONFIGURATION_TYPES="Debug" -G "Visual Studio 15 2017" -A x64
                    if (${'$'}LASTEXITCODE -ne 0) {
                        Write-Error "CMake configuration failed"
                        exit 1
                    }
    
                    cmake --build . -j8
                    if (${'$'}LASTEXITCODE -ne 0) {
                        Write-Error "CMake build failed"
                        exit 2
                    }
    
                    cmake --install .
                    if (${'$'}LASTEXITCODE -ne 0) {
                        Write-Error "CMake install failed"
                        exit 3
                    }
                """.trimIndent()
                }
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
            scriptContent = """
                mkdir %PATH__CRASH_DUMPS% 2>nul
                procdump -ma -e -t -x %PATH__CRASH_DUMPS% Debug\bin\ignite-client-test --gtest_output=xml:%PATH__CLIENT_TEST_RESULTS%
            """.trimIndent()
            formatStderrAsError = true
        }
        script {
            name = "ODBC integration tests"
            workingDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
            scriptContent = """
                mkdir %PATH__CRASH_DUMPS% 2>nul
                procdump -ma -e -t -x %PATH__CRASH_DUMPS% Debug\bin\ignite-odbc-test --gtest_output=xml:%PATH__ODBC_TEST_RESULTS%
            """.trimIndent()
            formatStderrAsError = true
        }
        powerShell {
            name = "Collect PDBs for crash dumps"
            platform = PowerShellStep.Platform.x64
            scriptMode = script {
                content = """ 
                    ${'$'}dumpsDir = "%PATH__CRASH_DUMPS%"
    
                    if (-not (Test-Path ${'$'}dumpsDir)) {
                        Write-Host "Dumps directory does not exist, skipping."
                        exit 0
                    }
    
                    ${'$'}dumps = Get-ChildItem -Path ${'$'}dumpsDir -File
                    if (${'$'}dumps.Count -eq 0) {
                        Write-Host "Dumps directory is empty, skipping."
                        exit 0
                    }
    
                    Write-Host "Found ${'$'}(${'$'}dumps.Count) dump file(s), collecting binaries from CMake build directory."
    
                    ${'$'}cmakeBuildDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
                    if (-not (Test-Path ${'$'}cmakeBuildDir)) {
                        Write-Error "CMake build directory '${'$'}cmakeBuildDir' does not exist."
                        exit 1
                    }
    
                    Get-ChildItem -Path "${'$'}cmakeBuildDir\*" -File -Include "*.exe", "*.dll", "*.pdb" | ForEach-Object {
                        Copy-Item -Path ${'$'}_.FullName -Destination ${'$'}dumpsDir -Force
                        Write-Host "Copied: ${'$'}(${'$'}_.Name)"
                    }
                """.trimIndent()
            }
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
