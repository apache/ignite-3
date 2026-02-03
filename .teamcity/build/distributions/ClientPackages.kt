package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.GradleBuildStep
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript

object ClientPackages : BuildType({
    name = "[11] ODBC and Client packages"
    description = "Apache Ignite 3 ODBC and Client Packages"

    artifactRules = """
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*odbc*.deb => odbc-deb
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*odbc*.deb.sha256 => odbc-deb
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*client*.deb => client-deb
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*client*.deb.sha256 => client-deb
        
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*odbc*.deb => odbc-rpm
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*odbc*.deb.sha256 => odbc-rpm
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*client*.deb => client-rpm
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*client*.deb.sha256 => client-rpm
        
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*odbc*.tar.gz => odbc-tgz
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*odbc*.tar.gz.sha256 => odbc-tgz
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*client*.tar.gz => client-tgz
        %VCSROOT__IGNITE3%/modules/platforms/build/cpp/_packages/*client*.tar.gz.sha256 => client-tgz
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
            """.trimIndent()
        }
        customGradle {
            name = "Build ODBC and Client packages. RPM, DEB, TGZ. (Under Rocky Linux 8 container)"
            tasks = ":platforms:cmakeCpack"
            workingDir = "%VCSROOT__IGNITE3%"
            gradleParams = "-i -Pplatforms.enable"
            dockerImage = "docker.gridgain.com/ci/tc-rockylinux8-odbc:v1.1"
            dockerPull = true
            dockerImagePlatform = GradleBuildStep.ImagePlatform.Linux
            dockerRunParameters = "-e JAVA_HOME=%CONTAINER_JAVA_HOME%"
        }
    }
})
