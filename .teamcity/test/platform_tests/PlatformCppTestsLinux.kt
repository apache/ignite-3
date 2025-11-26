package test.platform_tests

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildFeatures.XmlReport
import jetbrains.buildServer.configs.kotlin.buildFeatures.xmlReport
import jetbrains.buildServer.configs.kotlin.buildSteps.*
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText
import jetbrains.buildServer.configs.kotlin.failureConditions.failOnText
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.Teamcity


object PlatformCppTestsLinux : BuildType({
    id(Teamcity.getId(this::class))
    name = "Platform C++ Tests (Linux)"

    artifactRules = """
        %PATH__UNIT_TESTS_RESULT% => test_logs
        %PATH__CLIENT_TEST_RESULTS% => test_logs
        %PATH__CMAKE_BUILD_DIRECTORY%/core => core_dumps
    """.trimIndent()

    params {
        param("env.IGNITE_CPP_TESTS_USE_SINGLE_NODE", "")
        param("PATH__CMAKE_BUILD_DIRECTORY", "%PATH__WORKING_DIR%/cmake-build-debug")
        param("PATH__CLIENT_TEST_RESULTS", "%PATH__WORKING_DIR%/cpp_client_tests_results.xml")
        param("PATH__ODBC_TEST_RESULTS", "%PATH__WORKING_DIR%/odbc_tests_results.xml")
        param("PATH__UNIT_TESTS_RESULT", "%PATH__WORKING_DIR%/cpp_unit_test_results.xml")
        text("PATH__WORKING_DIR", "%teamcity.build.checkoutDir%/%VCSROOT__IGNITE3%/modules/platforms/cpp", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        param("env.CPP_STAGING", "/tmp/cpp_staging")
    }

    steps {
        script {
            name = "Build Info"
            workingDir = "%PATH__WORKING_DIR%"
            scriptContent = """
                gcc --version || exit 0
                g++ --version || exit 0

                odbcinst -j || exit 0
                cat /etc/odbcinst.ini || exit 0
            """.trimIndent()
        }
        script {
            name = "Build C++"
            scriptContent = """
                mkdir %PATH__CMAKE_BUILD_DIRECTORY%  || exit 2
                cd %PATH__CMAKE_BUILD_DIRECTORY%  || exit 3

                cmake .. -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DWARNINGS_AS_ERRORS=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=%env.CPP_STAGING% || (echo 'CMake configuration failed' && exit 5)
                cmake --build . -j8  || (echo 'CMake build failed' && exit 6)
                cmake --install . || (echo 'CMake install failed' && exit 7)
            """.trimIndent()
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
        customGradle {
            name = "Verify runner is builded"
            tasks = ":ignite-runner:integrationTestClasses"
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
        script {
            name = "Unit tests"
            workingDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
            scriptContent = """
                ctest --version
                ctest -E '(IgniteClientTest|IgniteOdbcTest)' --output-junit %PATH__UNIT_TESTS_RESULT%  || exit 0
            """.trimIndent()
            formatStderrAsError = true
        }
        script {
            name = "C++ Client integration tests"
            workingDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
            scriptContent = "./bin/ignite-client-test --gtest_output=xml:%PATH__CLIENT_TEST_RESULTS%"
            formatStderrAsError = true
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
        script {
            name = "ODBC integration tests"
            workingDir = "%PATH__CMAKE_BUILD_DIRECTORY%"
            scriptContent = """
                if [ -f "./bin/ignite-odbc-test" ]; then
                  ./bin/ignite-odbc-test --gtest_output=xml:%PATH__ODBC_TEST_RESULTS%
                fi
            """.trimIndent()
            formatStderrAsError = true
        }
        customScript(type = "bash") {
            name = "Clean Up Remaining Processes"
        }
    }

    failureConditions {
        executionTimeoutMin = 15
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
        failOnText {
            conditionType = BuildFailureOnText.ConditionType.CONTAINS
            pattern = "FAILED TEST SUITE"
            failureMessage = "One or several test suites have failed during SetUpTestSuite"
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

    /**
     *  Temporary lock Platform Linux jobs on old-type agents
     *  until execution of these tests is fixed on DIND agents
     */
    requirements {
        doesNotExist("env.DIND_ENABLED")
    }
})
