package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object OdbcZip : BuildType({
    name = "[9] ODBC Zip package"
    description = "Apache Ignite 3 ODBC Deb Package"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.zip => odbc-zip
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.tar => odbc-zip
    """.trimIndent()

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
            name = "Build Zip"
            tasks = ":packaging-odbc:distZip -i -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }

        customGradle {
            name = "Build Tar"
            id = "Build_Tar"
            tasks = ":packaging-odbc:distTar -i -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }

    requirements {
        equals("env.DIND_ENABLED", "true")
    }
})
