package test.platform_tests

import build.distributions.ClientPackages
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

object PlatformCppOdbcTestsLinux : BuildType({
    id(Teamcity.getId(this::class))
    name = "Platform C++ ODBC Tests (Linux)"

    artifactRules = """
        %PATH__CMAKE_BUILD_DIRECTORY%/core => core_dumps
    """.trimIndent()

    params {
        param("env.IGNITE_CPP_TESTS_USE_SINGLE_NODE", "")
        param("PATH__CMAKE_BUILD_DIRECTORY", "%PATH__WORKING_DIR%/cmake-build-debug")
        param("PATH__CMAKE_BUILD_DIRECTORY_ROCKYLINUX", "%PATH__WORKING_DIR%/cmake-build-debug-rockylinux")
        param("PATH__CMAKE_BUILD_DIRECTORY_UBUNTU", "%PATH__WORKING_DIR%/cmake-build-debug-ubuntu")
        param("PATH__ODBC_TEST_RESULTS_ROCKYLINUX", "%PATH__WORKING_DIR%/odbc_tests_results_rockylinux.xml")
        param("PATH__ODBC_TEST_RESULTS_UBUNTU", "%PATH__WORKING_DIR%/odbc_tests_results_ubuntu.xml")
        text("PATH__WORKING_DIR", "%teamcity.build.checkoutDir%/%VCSROOT__IGNITE3%/modules/platforms/cpp", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("PATH__IGNITE_DIR", "%teamcity.build.checkoutDir%/%VCSROOT__IGNITE3%", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        param("env.CPP_STAGING", "/tmp/cpp_staging")
        param("ROCKYLINUX_CONTAINER_JAVA_HOME", "/usr/lib/jvm/java-17-openjdk/")
        param("UBUNTU_CONTAINER_JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64/")
    }

    dependencies {
        dependency(ClientPackages) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = """
                    odbc-deb => ignite3-odbc-deb
                    odbc-rpm => ignite3-odbc-rpm
                    odbc-tgz => ignite3-odbc-tgz
                """.trimIndent()
            }
        }
    }

    steps {
        customScript(type = "bash") {
            name = "Setup Docker Proxy"
        }

        script {
            name = "Install ODBC and build C++ tests in Rockylinux 8 container"
            dockerImage = "docker.gridgain.com/ci/tc-rockylinux8-odbc:v1.1"
            dockerRunParameters = "-e JAVA_HOME=%ROCKYLINUX_CONTAINER_JAVA_HOME% --ulimit nofile=32768:32768"
            scriptContent = """
                clang --version
                clang++ --version
                ulimit -a

                rpm -i ignite3-odbc-rpm/*.rpm
                
                cd %PATH__IGNITE_DIR%
                ./gradlew :ignite-runner:integrationTestClasses
                mkdir %PATH__CMAKE_BUILD_DIRECTORY_ROCKYLINUX%  || exit 2
                cd %PATH__CMAKE_BUILD_DIRECTORY_ROCKYLINUX%  || exit 3

                cmake .. -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=%env.CPP_STAGING% || (echo 'CMake configuration failed' && exit 5)
                cmake --build . -j8  || (echo 'CMake build failed' && exit 6)
                                             
                ./bin/ignite-odbc-test --gtest_output=xml:%PATH__ODBC_TEST_RESULTS_ROCKYLINUX%
            """.trimIndent()
        }

        script {
            name = "Install ODBC and build C++ tests in Ubuntu 22.04 container"
            dockerImage = "docker.gridgain.com/ci/tc-ubuntu22_04-odbc:v1.0"
            dockerRunParameters = "-e JAVA_HOME=%UBUNTU_CONTAINER_JAVA_HOME% --ulimit nofile=32768:32768"
            scriptContent = """
                clang --version
                clang++ --version
                ulimit -a

                dpkg -i ignite3-odbc-deb/*.deb
                
                cd %PATH__IGNITE_DIR%
                ./gradlew :ignite-runner:integrationTestClasses
                mkdir %PATH__CMAKE_BUILD_DIRECTORY_UBUNTU%  || exit 2
                cd %PATH__CMAKE_BUILD_DIRECTORY_UBUNTU%  || exit 3

                cmake .. -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang -DENABLE_TESTS=ON -DENABLE_ODBC=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=%env.CPP_STAGING% || (echo 'CMake configuration failed' && exit 5)
                cmake --build . -j8  || (echo 'CMake build failed' && exit 6)
                                             
                ./bin/ignite-odbc-test --gtest_output=xml:%PATH__ODBC_TEST_RESULTS_UBUNTU%
            """.trimIndent()
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
                +:%PATH__ODBC_TEST_RESULTS_ROCKYLINUX%
                +:%PATH__ODBC_TEST_RESULTS_UBUNTU%
            """.trimIndent()
            verbose = true
        }
    }
})
