package test.template_types

import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import jetbrains.buildServer.configs.kotlin.BuildType

data class Tests(val configuration: TestConfiguration, val modules: List<GradleModule>) {

    fun buildType(): List<BuildType> {
        return modules.map {
            ApacheIgnite3CustomBuildType.Builder(TestsModule(configuration, it))
                .ignite3VCS().ignite3BuildDependency()
                .defaultBuildTypeSettings().requireLinux()
                .build().buildType
        }

    }
}