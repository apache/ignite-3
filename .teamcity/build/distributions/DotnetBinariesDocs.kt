package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildFeatures.nuGetPackagesIndexer
import jetbrains.buildServer.configs.kotlin.buildSteps.dotnetPack
import jetbrains.buildServer.configs.kotlin.buildSteps.dotnetPublish
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle


object DotnetBinariesDocs : BuildType({
    name = "[1] .NET Binaries | Docs"

    artifactRules = """
        %DIR__DOTNET% => %DIR__DOTNET%
        %DIR__NUGET% => %DIR__NUGET%
        +:%VCSROOT__IGNITE3%/modules/platforms/dotnet/docs => dotnetdoc.zip
    """.trimIndent()

    params {
        text("DIR__DOTNET", "dotnet", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("DIR__NUGET", "nuget", display = ParameterDisplay.HIDDEN, allowEmpty = true)
    }

    steps {
        dotnetPublish {
            name = "Build binaries"
            projects = "Apache.Ignite.sln"
            workingDir = "%VCSROOT__IGNITE3%/modules/platforms/dotnet"
            configuration = "Release"
            outputDir = "%teamcity.build.checkoutDir%/%DIR__DOTNET%"
        }
        dotnetPack {
            name = "Build Nuget"
            projects = "Apache.Ignite.sln"
            workingDir = "%VCSROOT__IGNITE3%/modules/platforms/dotnet"
            configuration = "Release"
            outputDir = "%teamcity.build.checkoutDir%/%DIR__NUGET%"
            args = "--include-source"
        }
        customGradle {
            name = "Build docfx"
            id = "Build_docfx"
            tasks = "docfx"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }

    features {
        nuGetPackagesIndexer {
            feed = "ignite3/ReleaseCandidate"
        }
    }
})
