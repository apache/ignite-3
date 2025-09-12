package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle

object Rpm : BuildType({
    name = "[2] RPM"

    artifactRules = """
        packaging/cli/build/distributions/*.rpm => rpm
        packaging/db/build/distributions/*.rpm => rpm
    """.trimIndent()

    steps {
        gradle {
            name = "Build RPM"
            tasks = "packaging-db:buildRpm -Pplatforms.enable"
        }
    }
})
