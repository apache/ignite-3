package _Self

import jetbrains.buildServer.configs.kotlin.DslContext
import jetbrains.buildServer.configs.kotlin.Project

/**
 * Variable to separate main (production) project from development projects
 */
var isActiveProject = DslContext.projectName == "[Apache Ignite 3.x]"

object Project : Project({
    subProject(build.Project)
    subProject(test.Project)
})
