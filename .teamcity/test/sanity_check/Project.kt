package test.sanity_check

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.sanity_check.build_types.*

object Project : Project({
    id(getId(this::class))
    name = "[Sanity Check]"

    listOf(
        AssembleTestClasses,
        CodeStyle,
        Javadoc,
        MentionTicket,
        PMD,
        Spotbugs,
    ).forEach {
        buildType(
            ApacheIgnite3CustomBuildType.Builder(it)
                .ignite3VCS().ignite3BuildDependency().setupMavenProxy()
                .defaultBuildTypeSettings().requireLinux()
                .build().buildType
        )
    }
})
