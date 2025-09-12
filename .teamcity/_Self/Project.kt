package _Self

import jetbrains.buildServer.configs.kotlin.DslContext
import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.Project

/**
 * Variable to separate main (production) project from development projects
 */
var isActiveProject: Boolean = DslContext.projectName == "[Apache Ignite 3.x]"

object Project : Project({
    subProject(build.Project)
    subProject(test.Project)

    /**
     * Project-wide params
     */
    params {
        param("system.lastCommitHash", "%build.vcs.number%")
        text("env.A_GRADLE_OPTS", "", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("env.GRADLE_OPTS", "-Dorg.gradle.caching=true %env.A_GRADLE_OPTS%", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("env.JAVA_HOME", "%env.JDK_ORA_17%", display = ParameterDisplay.HIDDEN, allowEmpty = true)
        text("env.M2_HOME", "%teamcity.tool.maven.DEFAULT%", display = ParameterDisplay.HIDDEN, allowEmpty = true)
    }
})
