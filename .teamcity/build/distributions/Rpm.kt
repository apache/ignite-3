package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object Rpm : BuildType({
    name = "[2] RPM"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/cli/build/distributions/*.rpm => rpm
        %VCSROOT__IGNITE3%/packaging/db/build/distributions/*.rpm => rpm
    """.trimIndent()

    steps {
        customGradle {
            name = "Build RPM"
            tasks = "packaging-db:buildRpm -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
