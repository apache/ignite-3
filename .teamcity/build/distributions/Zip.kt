package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle

object Zip : BuildType({
    name = "[6] All Zip"

    artifactRules = """
        packaging/db/build/distributions/*.zip => zip
        packaging/cli/build/distributions/*.zip => zip
        -:**/tmp/** => zip
    """.trimIndent()

    steps {
        gradle {
            name = "Build ZIP"
            tasks = "allDistZip -Pplatforms.enable"
        }
    }
})
