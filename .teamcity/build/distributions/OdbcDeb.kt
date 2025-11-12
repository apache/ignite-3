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
            name = "Install Conan"
            enabled = false
            scriptContent = """
                pip install wheel || exit 0
                pip install -v "conan>=1.56.0,<2.0.0" --force-reinstall  || exit 1
                
                ln -s /opt/buildagent/.local/bin/conan conan
            """.trimIndent()
        }
        script {
            name = "Check env"
            scriptContent = """
                gcc --version || exit 0
                g++ --version || exit 0
                
                odbcinst -j || exit 0
                cat /etc/odbcinst.ini || exit 0
                
                conan --version
                conan profile list
                conan profile show default || exit 0
                
                conan info --path . || exit 0
            """.trimIndent()
        }
        customGradle {
            name = "Build Deb"
            tasks = ":packaging-odbc:buildDeb -i -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }

    /**
     *  Temporary lock ODBC jobs on old-type agents
     *  until execution of the :platforms:cmakeBuildOdbc target is fixed on DIND agents
     */
    requirements {
        doesNotExist("env.DIND_ENABLED")
    }
})
