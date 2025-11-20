package build.build_types

import _Self.isActiveProject
import build.distributions.*
import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.CustomTriggers.Companion.customSchedule
import org.apache.ignite.teamcity.CustomTriggers.Companion.pullRequestChange
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object ReleaseBuild : BuildType({
    id(getId(this::class))
    name = "[1] Release Build"
    artifactRules = """
        **/*
        -: %VCSROOT__IGNITE3%
    """.trimIndent()

    triggers {
        customSchedule(0, "+:<default>", enabled = isActiveProject) {}
        pullRequestChange(enabled = isActiveProject) {
            triggerRules = """
                +:gradle/libs.versions.toml
                +:gradle/**
                +:packaging/**
            """.trimIndent()
            branchFilter = """
                +:pull/*
                +:dependabot/*
            """.trimIndent()
        }
    }

    steps {
        customScript(type = "bash") {
            name = "Clean up local maven repository"
        }

        customGradle {
            name = "Install to local repository"
            id = "Install_to_local_repository"
            tasks = "publishToMavenLocal"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }

    dependencies {
        dependency(CliDeb) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "cli-deb => ignite-cli-deb"
            }
        }
        dependency(CliRpm) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "cli-rpm => ignite-cli-rpm"
            }
        }
        dependency(Deb) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "deb => ignite-deb"
            }
        }
        dependency(Docs) {
            snapshot {}
            artifacts {
                artifactRules = "doxygen-cpp.zip => ignite-docs"
            }
        }
        dependency(DotnetBinariesDocs) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = """
                    nuget => ignite-nuget
                    dotnet => ignite-dotnet
                    dotnetdoc.zip => ignite-docs
                """.trimIndent()
            }
        }
        dependency(JavaBinariesDocs) {
            snapshot {}
            artifacts {
                artifactRules = """
                    javadoc.zip => ignite-docs
                    ignite-jdbc => ignite-jdbc
                    libs.zip
                """.trimIndent()
            }
        }
        dependency(OdbcDeb) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "odbc-deb => ignite-odbc-deb"
            }
        }
        dependency(OdbcRpm) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "odbc-rpm => ignite-odbc-rpm"
            }
        }
        dependency(OdbcZip) {
            snapshot {}
            artifacts {
                artifactRules = "odbc-zip => ignite-odbc-zip"
            }
        }
        dependency(OpenapiSpec) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "openapi.yaml => openapi"
            }
        }
        dependency(Rpm) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "rpm => ignite-rpm"
            }
        }
        dependency(Zip) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "zip => ignite-zip"
            }
        }
        dependency(MigrationToolsZip) {
            snapshot {}
            artifacts {
                cleanDestination = true
                artifactRules = "migration-tools-cli-zip => migration-tools-cli"
            }
        }
    }

})
