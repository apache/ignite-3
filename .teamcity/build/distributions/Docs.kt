package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object Docs : BuildType({
    name = "[10] C++ Docs"
    description = "Apache Ignite 3 OpenAPI specification"

    artifactRules = "%VCSROOT__IGNITE3%/modules/platforms/cpp/docs/html/ => doxygen-cpp.zip"

    steps {
        customGradle {
            tasks = "doxygenCppClient"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
