package org.apache.ignite.teamcity

import jetbrains.buildServer.configs.kotlin.Triggers
import jetbrains.buildServer.configs.kotlin.triggers.ScheduleTrigger
import jetbrains.buildServer.configs.kotlin.triggers.VcsTrigger

@Suppress("unused")
class CustomTriggers {
    companion object {
        /**
         * Custom SCHEDULE trigger
         *
         * @param triggerHour: hour
         * @param branchFilter: filter out target branch to trigger
         * @param enabled: enable/disable trigger
         */
        fun Triggers.customSchedule(
            triggerHour: Int,
            branchFilter: String,
            enabled: Boolean = true,
            init: ScheduleTrigger.() -> Unit
        ): ScheduleTrigger {
            val result = ScheduleTrigger(init)

            // Set hour in schedule (minutes default to 00)
            result.schedulingPolicy = result.daily {
                hour = triggerHour
            }
            // Allow to enable/disable trigger
            result.enabled = enabled
            // Filter branches (run on only specified ones)
            result.branchFilter = branchFilter
            // Run build despite the watching build is not changed
            result.triggerBuild = result.always()
            // Run build despite there are no pending changes in the build
            result.withPendingChangesOnly = false
            // Run build without build optimisation (stop if newer build is already running or finished)
            result.enableQueueOptimization = false
            // Force clean checkout (rebuild dependant builds)
            result.enforceCleanCheckoutForDependencies = true

            trigger(result)
            return result
        }


        /**
         * Trigger for changes in GitHub pull-request
         *
         * @param enabled: enable/disable trigger
         * @param quietPeriod: set custom quiet period time (seconds)
         */
        fun Triggers.pullRequestChange(
            enabled: Boolean = true,
            quietPeriod: Int = 300,
            init: VcsTrigger.() -> Unit
        ): VcsTrigger {
            val result = VcsTrigger(init)

            // Filter pull-requests
            result.branchFilter = "+:pull/*"
            // Allow to enable/disable trigger
            result.enabled = enabled
            result.enableQueueOptimization = true
            result.quietPeriodMode = VcsTrigger.QuietPeriodMode.USE_CUSTOM
            result.quietPeriod = quietPeriod

            trigger(result)
            return result
        }

        /**
         * Trigger for changes in Integration Branch
         *
         * @param enabled: enable/disable trigger
         * @param integrationBranchName: set custom branch name
         * @param quietPeriod: set custom quiet period time (seconds)
         */
        fun Triggers.integrationBranchChange(
            enabled: Boolean = true,
            integrationBranchName: String = "main",
            quietPeriod: Int = 300,
            init: VcsTrigger.() -> Unit
        ): VcsTrigger {
            val result = VcsTrigger(init)

            // Trigger only on integration branch
            result.branchFilter = "+:${integrationBranchName}"
            // Allow to enable/disable trigger
            result.enabled = enabled
            result.enableQueueOptimization = true
            result.quietPeriodMode = VcsTrigger.QuietPeriodMode.USE_CUSTOM
            result.quietPeriod = quietPeriod

            trigger(result)
            return result
        }
    }
}