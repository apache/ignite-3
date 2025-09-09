package build.distributions

import jetbrains.buildServer.configs.kotlin.*
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object CliRpm : BuildType({
    name = "[5] CLI RPM"

    artifactRules = "%VCSROOT__IGNITE3%/packaging/cli/build/distributions/*.rpm => cli-rpm"

    params {
        text("DIR_BUILD", "deliveries/deb", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_BINARIES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_PACKAGES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
    }

    steps {
        customGradle {
            name = "Build RPM"
            tasks = "packaging-cli:buildRpm"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
