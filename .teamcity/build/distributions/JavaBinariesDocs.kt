package build.distributions


import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object JavaBinariesDocs : BuildType({
    name = "[1] Java Binaries | Docs"

    artifactRules = """
        +:build/docs => javadoc.zip
        +:modules/*/build/libs/** => libs.zip
        +:modules/jdbc/build/libs/*-all.jar => ignite-jdbc
    """.trimIndent()

    steps {
        customGradle {
            name = "Build | Assemble binaries"
            tasks = "jar shadowJar"
            workingDir = "%VCSROOT__IGNITE3%"
        }
        customGradle {
            name = "Aggregate Javadocs"
            id = "gradle_runner"
            tasks = "aggregateJavadoc"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
