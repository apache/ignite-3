package build.distributions

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle

object CliRpm : BuildType({
    name = "[5] CLI RPM"

    artifactRules = "packaging/cli/build/distributions/*.rpm => cli-rpm"

    params {
        text("DIR_BUILD", "deliveries/deb", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_BINARIES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_PACKAGES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
    }

    steps {
        gradle {
            name = "Build RPM"
            tasks = "packaging-cli:buildRpm"
        }
    }
})
