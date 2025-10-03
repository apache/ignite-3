package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object CliDeb : BuildType({
    name = "[4] CLI DEB"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/cli/build/distributions/*.deb => cli-deb
        %VCSROOT__IGNITE3%/packaging/cli/build/distributions/*.changes => cli-deb
    """.trimIndent()

    params {
        text("DIR_BUILD", "deliveries/deb", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_BINARIES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_PACKAGES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
    }

    steps {
        customGradle {
            name = "Build DEB"
            tasks = "packaging-cli:buildDeb"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
