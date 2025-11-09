package test.template_types

import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import jetbrains.buildServer.configs.kotlin.BuildType

data class Tests(
    val configuration: TestConfiguration,
    val modules: List<GradleModule>,
    val enableOthers: Boolean = true,
    val excludeOnlyModules: List<GradleModule> = emptyList(),
) {

    fun buildType(): List<BuildType> {
        val map = modules.map {
            ApacheIgnite3CustomBuildType.Builder(TestsModule(configuration, it))
                .ignite3VCS().ignite3BuildDependency()
                .defaultBuildTypeSettings().requireLinux()
                .testsFailureCondition()
                .build().buildType
        }

        if (enableOthers) {
            val otherModules = ApacheIgnite3CustomBuildType.Builder(OtherTestsModule(configuration, modules + excludeOnlyModules))
                .ignite3VCS().ignite3BuildDependency()
                .defaultBuildTypeSettings().requireLinux()
                .testsFailureCondition()
                .build().buildType

            return map + otherModules
        }
        return map
    }
}