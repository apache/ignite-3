package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object OpenapiSpec : BuildType({
    name = "[10] OpenAPI specification"
    description = "Apache Ignite 3 OpenAPI specification"

    artifactRules = "modules/rest-api/build/openapi/openapi.yaml => artifacts"

    steps {
        script {
            name = "Check env"
            scriptContent = """
                gcc --version || exit 0
                g++ --version || exit 0
                
                odbcinst -j || exit 0
                cat /etc/odbcinst.ini || exit 0
                
                conan --version
                conan profile list
                conan profile show default || exit 0
                
                conan info --path . || exit 0
            """.trimIndent()
        }
        gradle {
            tasks = ":ignite-rest-api:compileJava"
        }
    }
})
