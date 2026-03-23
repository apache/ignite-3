package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object OpenapiSpec : BuildType({
    name = "[10] OpenAPI specification"
    description = "Apache Ignite 3 OpenAPI specification"

    artifactRules = "%VCSROOT__IGNITE3%/modules/rest-api/build/openapi/openapi.yaml => openapi.yaml"

    steps {
        customGradle {
            tasks = ":ignite-rest-api:compileJava"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
