package _Self

import jetbrains.buildServer.configs.kotlin.DslContext
import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenText

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
        hiddenText("IGNITE_CI", "true")
        hiddenText("env.A_GRADLE_OPTS", "")
        hiddenText("env.GRADLE_OPTS", "-Dorg.gradle.caching=true %env.A_GRADLE_OPTS%")
        hiddenText("env.JAVA_HOME", "%env.JDK_ORA_17%")
        hiddenText("env.M2_HOME", "%teamcity.tool.maven.DEFAULT%")
    }
})
