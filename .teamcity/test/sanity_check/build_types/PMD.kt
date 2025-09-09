package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object PMD : BuildType({
    id(getId(this::class))
    name = "PMD"
    description = "Check possible bugs on code using PMD"

    artifactRules = "**/build/reports/pmd/*"

    steps {
        customGradle {
            name = "PMD check"
            tasks = "pmdMain pmdTest"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})
