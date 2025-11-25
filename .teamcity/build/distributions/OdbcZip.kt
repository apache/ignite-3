package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.GradleBuildStep
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object OdbcZip : BuildType({
    name = "[9] ODBC Zip package"
    description = "Apache Ignite 3 ODBC Deb Package"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.zip => odbc-zip
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.tar => odbc-zip
    """.trimIndent()

    params {
        param("CONTAINER_JAVA_HOME", "/usr/lib/jvm/java-17-openjdk/")
    }

    steps {
        script {
            name = "Check env"
            scriptContent = """
                gcc --version || exit 0
                g++ --version || exit 0
                
                odbcinst -j || exit 0
                cat /etc/odbcinst.ini || exit 0
            """.trimIndent()
        }

        customGradle {
            name = "Build ODBC Zip (Under Rocky Linux 8 container)"
            tasks = ":packaging-odbc:distZip"
            workingDir = "%VCSROOT__GRIDGAIN9%"
            gradleParams = "-i -Pplatforms.enable"
            dockerImage = "docker.gridgain.com/ci/tc-rockylinux8-odbc:v1.0"
            dockerPull = true
            dockerImagePlatform = GradleBuildStep.ImagePlatform.Linux
            dockerRunParameters = "-e JAVA_HOME=%CONTAINER_JAVA_HOME%"
        }
        customGradle {
            name = "Build ODBC Tar (Under Rocky Linux 8 container)"
            tasks = ":packaging-odbc:distTar"
            workingDir = "%VCSROOT__GRIDGAIN9%"
            gradleParams = "-i -Pplatforms.enable"
            dockerImage = "docker.gridgain.com/ci/tc-rockylinux8-odbc:v1.0"
            dockerPull = true
            dockerImagePlatform = GradleBuildStep.ImagePlatform.Linux
            dockerRunParameters = "-e JAVA_HOME=%CONTAINER_JAVA_HOME%"
        }
    }

    requirements {
        equals("env.DIND_ENABLED", "true")
    }
})
