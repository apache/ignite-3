package test.sanity_check.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customGradle
import org.apache.ignite.teamcity.Teamcity.Companion.getId

object AssembleTestClasses : BuildType({
    id(getId(this::class))
    name = "Assemble testClasses [JDK11]"
    description = "Assemble testClasses on JDK11"

    steps {
        customGradle {
            name = "Assemble testClasses on JDK11"
            tasks = "assemble testClasses"
            jdkHome = "%env.JDK_ORA_11%"
            workingDir = "%VCSROOT__IGNITE3%"
        }
    }
})