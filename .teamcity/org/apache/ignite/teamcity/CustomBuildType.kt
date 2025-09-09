package org.apache.ignite.teamcity

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.BuildTypeSettings.Type
import jetbrains.buildServer.configs.kotlin.CheckoutMode

@Suppress("unused")
open class CustomBuildType(open val buildType: BuildType) {
    open class Builder(open var buildType: BuildType) {
        /**
         * Default settings for every Apache Ignite 3 TeamCity build configuration
         */
        @Suppress("unused")
        fun defaultBuildTypeSettings() = apply {
            buildType.vcs {
                checkoutMode = if (checkoutMode == CheckoutMode.AUTO) CheckoutMode.ON_SERVER else checkoutMode
                cleanCheckout = true
                showDependenciesChanges = true
            }

            buildType.failureConditions {
                executionTimeoutMin = if (buildType.type == Type.COMPOSITE) 0
                else if (executionTimeoutMin == 0) 30 else executionTimeoutMin
            }
        }


        /**
         * Linux-based agent requirement
         */
        @Suppress("unused")
        fun requireLinux() = apply {
            buildType.requirements {
                equals("teamcity.agent.jvm.os.name", "Linux")
            }
        }


        /**
         * Linux-based agent requirement
         */
        @Suppress("unused")
        fun requireWindows() = apply {
            buildType.requirements {
                startsWith("teamcity.agent.jvm.os.name", "Windows")
            }
        }


        /**
         * Clean all build run information in 24h
         */
        @Suppress("unused")
        fun immediateCleanup() = apply {
            buildType.cleanup {
                baseRule {
                    all(days = 1)
                }
            }
        }


        /**
         * Return updated BuildType object
         */
        fun build() = CustomBuildType(buildType)
    }
}