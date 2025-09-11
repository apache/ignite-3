package test.template_types

import _Self.OsArch

data class TestConfiguration(
    val suiteId: String,
    val testTask: String,
    val xmx: Int = 2,
    val executionTimeoutMin: Int = 30,
    val jvmArg: String = "",
    val osArch: OsArch = OsArch.LinuxAMD64,
    val dindSupport: Boolean = false)