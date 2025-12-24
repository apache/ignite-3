package build.distributions

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId

object Project : Project({
    id(getId(this::class))
    name = "[Distributions]"

    /**
     * Full list of all product distributions
     */

    listOf(
        CliDeb,
        CliRpm,
        Deb,
        Docs,
        DotnetBinariesDocs,
        JavaBinariesDocs,
        OdbcDeb,
        OdbcRpm,
        OdbcZip,
        OpenapiSpec,
        Rpm,
        Zip,
        MigrationToolsZip
    ).forEach {
        buildType(
            ApacheIgnite3CustomBuildType.Builder(it)
                .ignite3VCS().ignite3BuildDependency().setupMavenProxy()
                .defaultBuildTypeSettings().requireLinux()
                .build().buildType
        )
    }
})
