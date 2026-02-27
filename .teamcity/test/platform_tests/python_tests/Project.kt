package test.platform_tests.python_tests

import jetbrains.buildServer.configs.kotlin.Project
import org.apache.ignite.teamcity.ApacheIgnite3CustomBuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId

object Project : Project({
    id(getId(this::class))
    name = "[Python Tests]"

    listOf(
            Triple("3.10", "py310", "Python DB API Tests - Python 3.10"),
            Triple("3.11", "py311", "Python DB API Tests - Python 3.11"),
            Triple("3.12", "py312", "Python DB API Tests - Python 3.12"),
            Triple("3.13", "py313", "Python DB API Tests - Python 3.13"),
            Triple("3.14", "py314", "Python DB API Tests - Python 3.14"),
            Triple("3.14t", "py314", "Python DB API Tests (No GIL) - Python 3.14"),
    ).forEach { (ver, toxEnv, name) ->
        buildType(
            ApacheIgnite3CustomBuildType.Builder(PythonDbApiToxTest(ver, toxEnv, name))
                .ignite3VCS().ignite3CommitStatusPublisher()
                .defaultBuildTypeSettings().requireLinux()
                .build().buildType
        )
    }
})

