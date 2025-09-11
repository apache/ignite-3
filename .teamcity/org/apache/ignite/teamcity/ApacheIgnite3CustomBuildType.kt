package org.apache.ignite.teamcity

import build.build_types.ApacheIgnite3
import jetbrains.buildServer.configs.kotlin.AbsoluteId
import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.FailureAction
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText


class ApacheIgnite3CustomBuildType(override val buildType: BuildType) : CustomBuildType(buildType) {
    class Builder(override var buildType: BuildType) : CustomBuildType.Builder(buildType) {
        /**
         * Apache Ignite 3 VCS settings
         */
        fun ignite3VCS() = apply {
            buildType.params {
                hiddenText("VCSROOT__IGNITE3", "%teamcity.build.checkoutDir%/ignite-3")
                hiddenText("env.JAVA_HOME", "%env.JDK_ORA_11%")
                hiddenText("env.GRADLE_OPTS", "-Dorg.gradle.caching=false")
            }
            buildType.vcs {
                root(AbsoluteId("GitHubApacheIgnite3"), "+:. => %VCSROOT__IGNITE3%")
            }
        }


        /**
         * Use pre-built Apache Ignite 3 artifacts instead of rebuilding project again
         */
        fun ignite3BuildDependency() = apply {
            buildType.dependencies {
                snapshot(ApacheIgnite3) {
                    onDependencyFailure = FailureAction.FAIL_TO_START
                }
            }
        }
    }
}
