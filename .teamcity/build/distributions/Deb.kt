package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object Deb : BuildType({
    name = "[3] DEB"

    artifactRules = """
        %VCSROOT__IGNITE3%/packaging/db/build/distributions/*.deb => deb
        %VCSROOT__IGNITE3%/packaging/db/build/distributions/*.changes => deb
    """.trimIndent()

    steps {
        customGradle {
            name = "Build DEB"
            tasks = "packaging-db:buildDeb -Pplatforms.enable"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
