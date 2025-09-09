package test.sanity_check

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.sanity_check.build_types.CodeStyle
import test.sanity_check.build_types.Inspections
import test.sanity_check.build_types.Javadoc
import test.sanity_check.build_types.PMD

object Project : Project({
    id(getId(this::class))
    name = "[Sanity Check]"

    listOf(
        CodeStyle,
        Inspections,
        Javadoc,
        PMD
    ).forEach {
        buildType(
            ApacheIgnite3CustomBuildType.Builder(it)
                .ignite3VCS().ignite3BuildDependency()
                .defaultBuildTypeSettings().requireLinux()
                .build().buildType
        )
    }
})
