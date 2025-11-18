package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object OdbcDeb : BuildType({
    name = "[7] ODBC Deb package"
    description = "Apache Ignite 3 ODBC Deb Package"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.deb => odbc-deb
        %VCSROOT__IGNITE3%/packaging/odbc/build/distributions/*.changes => odbc-deb
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
            name = "Build Deb"
            tasks = ":packaging-odbc:buildDeb -i -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }

    requirements {
        equals("env.DIND_ENABLED", "true")
    }
})
