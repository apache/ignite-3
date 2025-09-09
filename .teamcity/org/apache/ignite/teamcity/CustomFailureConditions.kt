package org.apache.ignite.teamcity

import jetbrains.buildServer.configs.kotlin.FailureConditions
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText


@Suppress("unused")
class CustomFailureConditions {
    companion object {
        fun FailureConditions.failOnExactText(
            pattern: String,
            failureMessage: String,
            init: BuildFailureOnText.() -> Unit
        ): BuildFailureOnText {
            val result = BuildFailureOnText(init)

            result.conditionType = BuildFailureOnText.ConditionType.CONTAINS
            result.pattern = pattern
            result.failureMessage = failureMessage
            result.reverse = false
            result.reportOnlyFirstMatch = false

            failureCondition(result)
            return result
        }
    }
}