package org.apache.ignite.teamcity

import jetbrains.buildServer.configs.kotlin.FailureConditions
import jetbrains.buildServer.configs.kotlin.failureConditions.BuildFailureOnText


@Suppress("unused")
class CustomFailureConditions {
    companion object {
        fun FailureConditions.failOnExactText(
                text: String,
                init: BuildFailureOnText.() -> Unit
        ): BuildFailureOnText {
            val result = BuildFailureOnText(init)

            result.conditionType = BuildFailureOnText.ConditionType.CONTAINS
            result.pattern = text
            result.failureMessage = text
            result.reverse = false

            failureCondition(result)
            return result
        }
    }
}