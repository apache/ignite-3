package org.apache.ignite.teamcity

import jetbrains.buildServer.configs.kotlin.ParameterDisplay
import jetbrains.buildServer.configs.kotlin.ParametrizedWithType
import kotlin.reflect.KClass


class Teamcity {
    companion object {
        /*************
         * FUNCTIONS *
         *************/
        /**
         * Convert step name to script file name
         */
        fun getBashScriptFileName(name: String): String {
            // Translate step name to script name
            val array = name.replace("""[()+.:-]""".toRegex(), " ").split(" ").toTypedArray()
            for (i in array.indices) { array[i] = array[i].replaceFirstChar { it.uppercase() } }

            return array.joinToString(separator = "").replace("/", "_")
        }


        /**
         * Convert Project or BuildType fully-qualified class name into ID
         */
        @Suppress("unused", "unused")
        fun getId(kClass: KClass<*>, customClassName: String? = null, isParametrizedClass: Boolean = false): String {
            // Disassemble qualified class name into packages and class name
            val list = kClass.qualifiedName.toString().split(".").toMutableList()

            // Remove Project parts of qualified class name which do not participate in id naming
            arrayOf(
                "Project",
                "build_types", "_build_types",
                "template_types", "_template_types"
            ).forEach { list.remove(it) }
            if (isParametrizedClass) list.removeLast()

            // Process every element of list
            for (i in list.indices) {
                // Convert class names with underscore
                val sublist = list[i].split("_").toMutableList()
                for (j in sublist.indices) { sublist[j] = sublist[j].replaceFirstChar { it.uppercase() } }
                list[i] = sublist.joinToString("")

                // Capitalized every element of list
                list[i] = list[i].replaceFirstChar { it.uppercase() }
            }

            // Replace class name with custom
            if (customClassName != null) {
                list.add(customClassName.replace("""[ &=+./>:()-]""".toRegex(), ""))
//                list[list.lastIndex] = customClassName.replace("""[ &=+./>:()-]""".toRegex(), "")
            }

            return list.joinToString(separator = "_")
        }


        /*****************
         * CUSTOM PARAMS *
         *****************/
        /**
         * Hidden text field
         */
        @Suppress("MemberVisibilityCanBePrivate")
        fun ParametrizedWithType.hiddenText(name: String, value: String) {
            text(name, value, display = ParameterDisplay.HIDDEN, allowEmpty = true)
        }


        /**
         * Hidden password field
         */
        @Suppress("unused")
        fun ParametrizedWithType.hiddenPassword(name: String, value: String) {
            password(name, value, display = ParameterDisplay.HIDDEN)
        }

        /**
        * Custom constructable text param for PATH environment variable
         * Provides ability to add param with several additional paths added to env.PATH depending on required binary
         *
         * @param binList list of possible binaries to add to PATH. Valid values:
         *  - maven
         */
        @Suppress("unused")
        fun ParametrizedWithType.customPATH(binList: List<String>) {
            val pathList: MutableList<String> = mutableListOf()

            // Add matching paths to list of paths
            binList.forEach {
                when (it) { "maven" -> pathList.add("%teamcity.tool.maven.DEFAULT%/bin") }
            }

            // Add all other paths (to avoid PATH override)
            pathList.add("%env.PATH%")

            hiddenText("env.PATH", pathList.joinToString(separator = ":"))
        }


        /**
         * Adds reverse dependency param assignment for usage inside build configuration
         *
         * @param buildTypeClass 'BuildType' class instance
         * @param paramName name of parameter to add reverse dependency to
         *
         * @return param name with reverse dependency prefix
         */
        @Suppress("unused")
        fun addReverseParam(buildTypeClass: CustomBuildType.Builder, paramName: String, direct: Boolean): String {
            val reverseParamName = "rev.dep.*.$paramName"
            if (direct) buildTypeClass.apply { buildType.params { hiddenText(paramName, reverseParamName) } }

            return reverseParamName
        }
    }
}
