package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import jetbrains.buildServer.configs.kotlin.buildSteps.script

object OpenapiSpec : BuildType({
    name = "[10] OpenAPI specification"
    description = "Apache Ignite 3 OpenAPI specification"

    artifactRules = "%VCSROOT__IGNITE3%/modules/rest-api/build/openapi/openapi.yaml => artifacts"

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
        customGradle {
            tasks = ":ignite-rest-api:compileJava"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
