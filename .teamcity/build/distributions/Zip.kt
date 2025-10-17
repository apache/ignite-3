package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object Zip : BuildType({
    name = "[6] All Zip"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/db/build/distributions/*.zip => zip
        %VCSROOT__IGNITE3%/packaging/cli/build/distributions/*.zip => zip
        -:**/tmp/** => zip
    """.trimIndent()

    steps {
        customGradle {
            name = "Build ZIP"
            tasks = "allDistZip -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
