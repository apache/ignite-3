package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle

object CliDeb : BuildType({
    name = "[4] CLI DEB"

    artifactRules = """
        packaging/cli/build/distributions/*.deb => cli-deb
        packaging/cli/build/distributions/*.changes => cli-deb
    """.trimIndent()

    params {
        text("DIR_BUILD", "deliveries/deb", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_BINARIES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR_PACKAGES", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
    }

    steps {
        gradle {
            name = "Build DEB"
            tasks = "packaging-cli:buildDeb"
        }
    }
})
