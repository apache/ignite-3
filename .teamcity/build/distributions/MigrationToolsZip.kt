package build.distributions

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle

object MigrationToolsZip : BuildType({
    name = "Migration Tools CLI ZIP"

    artifactRules = """
        %VCSROOT__IGNITE3%/migration-tools/packaging/cli/build/distributions/*.zip => migration-tools-cli-zip
        -:**/tmp/** => zip
    """.trimIndent()

    steps {
        customGradle {
            name = "Build Migration Tools ZIP"
            tasks = "migration-tools-packaging-cli:distZip"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
