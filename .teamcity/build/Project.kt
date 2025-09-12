package build

import build.build_types.ApacheIgnite3
import build.build_types.ReleaseBuild
import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId


object Project : Project({
    id(getId(this::class))
    name = "[Build]"

    subProject(build.distributions.Project)

    /**
     * Pre-compiling ignite for consistent caching
     */

    buildType(
        ApacheIgnite3CustomBuildType.Builder(ApacheIgnite3)
            .ignite3VCS()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )

    /**
     * Aggregating pipeline that collects all distributions
     */

    buildType(
        ApacheIgnite3CustomBuildType.Builder(ReleaseBuild)
            .ignite3VCS().ignite3CommitStatusPublisher()
            .defaultBuildTypeSettings().requireLinux()
            .build().buildType
    )
})
