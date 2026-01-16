package test.template_types

/**
 * @param parallelTestsEnabled split tests of a build into several batches, by test classes and run them in parallel on suitable build agents.
 *        See [jetbrains.buildServer.configs.kotlin.buildFeatures.ParallelTestsFeature].
 */
data class GradleModule(val displayName: String, val moduleName: String, val jvmArgs: String = "", val dependencies: List<String> = emptyList(), val parallelTestsEnabled: Boolean = false) {
    fun buildTask(taskName: String): String {
        val dependencyTasks = dependencies.joinToString(" ")
        val mainTask = ":$moduleName:$taskName"
        return if (dependencyTasks.isNotEmpty()) "$dependencyTasks $mainTask" else mainTask
    }
}
