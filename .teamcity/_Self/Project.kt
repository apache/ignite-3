package _Self

import jetbrains.buildServer.configs.kotlin.DslContext
import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.Teamcity.Companion.hiddenPassword
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
        hiddenText("DOCKERPROXY_USERNAME", "robot\$ignite-3")
        hiddenPassword("DOCKERPROXY_PASSWORD", "credentialsJSON:56ca9b55-a5ad-4244-a5c1-5b4f062366dd")
        hiddenText("system.lastCommitHash", "%build.vcs.number%")
        hiddenText("IGNITE_CI", "true")
        hiddenText("env.GRADLE_OPTS", "-Dorg.gradle.caching=true -Dorg.gradle.java.home=%env.JDK_ORA_17%")
        hiddenText("env.M2_HOME", "%teamcity.tool.maven.DEFAULT%")
    }
})
