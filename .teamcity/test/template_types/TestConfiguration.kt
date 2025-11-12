package test.template_types

data class TestConfiguration(
    val suiteId: String,
    val testTask: String,
    val xmx: Int = 2,
    val executionTimeoutMin: Int = 30,
    val jvmArg: String = "",
    val dindSupport: Boolean = false)