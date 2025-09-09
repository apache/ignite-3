package test.template_types

data class GradleModule(val displayName: String, val moduleName: String, val jvmArgs: String = "") {
    fun buildTask(taskName: String): String {
        return ":$moduleName:$taskName"
    }
}
