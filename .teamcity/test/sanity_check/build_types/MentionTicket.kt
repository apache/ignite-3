package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.buildSteps.maven
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customScript
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object MentionTicket : BuildType({
    id(getId(this::class))
    name = "Mention ticket"

    steps {
        customScript(type = "bash") {
            name = "Check code base"
            id = "CheckCodeBase"
            workingDir = "%VCSROOT__IGNITE3%"

            conditions {
                equals("teamcity.build.branch.is_default", "false")
            }
        }
    }
})
