package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle

object Docs : BuildType({
    name = "[10] C++ Docs"
    description = "Apache Ignite 3 OpenAPI specification"

    artifactRules = "modules/platforms/cpp/docs/html/ => doxygen-cpp.zip"

    steps {
        gradle {
            tasks = "doxygenCppClient"
        }
    }
})
