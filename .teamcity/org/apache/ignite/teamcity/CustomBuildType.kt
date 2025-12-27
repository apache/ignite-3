package org.apache.ignite.teamcity

import jetbrains.buildServer.configs.kotlin.BuildType
import jetbrains.buildServer.configs.kotlin.BuildTypeSettings.Type
import jetbrains.buildServer.configs.kotlin.CheckoutMode
import org.apache.ignite.teamcity.CustomBuildSteps.Companion.customPython
import org.apache.ignite.teamcity.CustomFailureConditions.Companion.failOnExactText

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
         * Non docker-in-docker-based agent requirement
         */
        @Suppress("unused")
        fun requireNonDind() = apply {
            buildType.requirements {
                doesNotExist("env.DIND_ENABLED")
            }
        }


        /**
         * Docker-in-docker-based agent requirement
         */
        @Suppress("unused")
        fun requireDind() = apply {
            buildType.requirements {
                equals("env.DIND_ENABLED", "true")
            }
        }


        /**
         * Windows-based agent requirement
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
         * Apply additional failure conditions for tests based on messages in the build log
         */
        fun testsFailureCondition() = apply {
            buildType.failureConditions {
                failOnExactText(pattern = "LEAK:", failureMessage = "Netty buffer leak detected.") {}
                failOnExactText(pattern = "java.lang.NullPointerException", failureMessage = "NullPointerException detected.") {}
                failOnExactText(pattern = "java.lang.AssertionError", failureMessage = "AssertionError detected.") {}
                failOnExactText(pattern = "Critical system error detected.", failureMessage = "Critical system error detected.") {}
            }
        }


        /**
         * Replace maven repos with proxy repositories
         */
        fun setupMavenProxy() = apply {
            buildType.steps {
                customPython {
                    name = "Setup Maven Proxy"
                    workingDir = "%VCSROOT__IGNITE3%"
                }
                items.add(0, items[items.lastIndex])
                items.removeAt(items.lastIndex)
            }
        }

        /**
         * Return updated BuildType object
         */
        fun build() = CustomBuildType(buildType)
    }
}