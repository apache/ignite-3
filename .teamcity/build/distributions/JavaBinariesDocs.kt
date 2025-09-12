package build.distributions


import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.buildSteps.gradle

object JavaBinariesDocs : BuildType({
    name = "[1] Java Binaries | Docs"

    artifactRules = """
        +:build/docs => javadoc.zip
        +:modules/*/build/libs/** => libs.zip
        +:modules/jdbc/build/libs/*-all.jar => ignite-jdbc
    """.trimIndent()

    steps {
        gradle {
            name = "Build | Assemble binaries"
            tasks = "jar shadowJar"
        }
        gradle {
            name = "Aggregate Javadocs"
            id = "gradle_runner"
            tasks = "aggregateJavadoc"
        }
    }
})
