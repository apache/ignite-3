package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle

object Deb : BuildType({
    name = "[3] DEB"

    artifactRules = """
        packaging/db/build/distributions/*.deb => deb
        packaging/db/build/distributions/*.changes => deb
    """.trimIndent()

    steps {
        gradle {
            name = "Build DEB"
            tasks = "packaging-db:buildDeb -Pplatforms.enable"
        }
    }
})
