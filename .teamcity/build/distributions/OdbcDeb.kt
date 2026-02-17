package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.GradleBuildStep
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript

object OdbcDeb : BuildType({
    name = "[7] ODBC Deb package"
    description = "Apache Ignite 3 ODBC Deb Package"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.deb => odbc-deb
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.changes => odbc-deb
    """.trimIndent()

    params {
        param("CONTAINER_JAVA_HOME", "/usr/lib/jvm/java-17-openjdk/")
    }

    steps {
        customScript(type = "bash") {
            name = "Setup Docker Proxy"
        }

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
            name = "Build ODBC DEB (Under Rocky Linux 8 container)"
            tasks = ":packaging-odbc:buildDeb"
            workingDir = "%VCSROOT__IGNITE3%"
            gradleParams = "-i -Pplatforms.enable"
            dockerImage = "docker.gridgain.com/ci/tc-rockylinux8-odbc:v1.1"
            dockerPull = true
            dockerImagePlatform = GradleBuildStep.ImagePlatform.Linux
            dockerRunParameters = "-e JAVA_HOME=%CONTAINER_JAVA_HOME%"
        }
    }

    requirements {
        equals("env.DIND_ENABLED", "true")
    }
})
