package _Self

import jetbrains.buildServer.configs.kotlin.DslContext
import jetbrains.buildServer.configs.kotlin.Project

/**
 * Variable to separate main (production) project from development projects
 */
var isActiveProject: Boolean = DslContext.projectName == "[Apache Ignite 3.x]"

enum class OsArch(val os: String, val arch: String) {
    LinuxAMD64("Linux", "amd64"),
    WindowsAMD64("Windows 10", "amd64");

    fun displayName(): String = "$os $arch"
}

object Project : Project({
    subProject(build.Project)
    subProject(test.Project)
})
