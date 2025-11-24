package build.distributions


import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.ExecBuildStep
import jetbrains.buildServer.configs.kotlin.buildSteps.exec
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object OdbcRpm : BuildType({
    name = "[8] ODBC RPM package"
    description = "Apache Ignite 3 ODBC RPM package"

    artifactRules = "%VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.rpm=> odbc-rpm"

    params {
        param("CONTAINER_JAVA_HOME", "/usr/lib/jvm/java-17-openjdk/")
        hiddenText("AGENT_NUMBER", "")
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

        script {
            name = "[HACK] Set AGENT_NUMBER"
            id = "HACK_Set_AGENT_NUMBER"
            enabled = true
            scriptContent = """
                AGENT_NUMBER=${'$'}(echo %system.agent.name% | tail -c 3)
                echo "##teamcity[setParameter name='AGENT_NUMBER' value='${'$'}{AGENT_NUMBER}']"
            """.trimIndent()
        }

        exec {
            name = "Build ODBC RPM (Under Rocky Linux 8 container)"
            id = "Build_ODBC_RPM_Under_Rocky_Linux_8_container"
            enabled = true
            path = "./gradlew"
            arguments = ":packaging-odbc:buildRpm -i -Pplatforms.enable"
            dockerImage = "ggshared/tc-agent:rockylinux_latest"
            dockerImagePlatform = ExecBuildStep.ImagePlatform.Linux
            dockerPull = true
            dockerRunParameters = "-e JAVA_HOME=%CONTAINER_JAVA_HOME% -v /mnt/teamcity/%AGENT_NUMBER%/work/%teamcity.build.default.checkoutDir%:%teamcity.build.checkoutDir%"
            param("script.content", "./gradlew")
        }
    }

    requirements {
        equals("env.DIND_ENABLED", "true")
    }
})
