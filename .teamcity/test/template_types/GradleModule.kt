package test.template_types

data class GradleModule(val displayName: String, val moduleName: String, val jvmArgs: String = "", val dependencies: List<String> = emptyList()) {
    fun buildTask(taskName: String): String {
        val dependencyTasks = dependencies.joinToString(" ")
        val mainTask = ":$moduleName:$taskName"
        return if (dependencyTasks.isNotEmpty()) "$dependencyTasks $mainTask" else mainTask
    }
}
