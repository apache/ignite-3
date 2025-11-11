package test.template_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId

class RunTestsList(private val testsList: List<Tests>, private val suiteId: String) : BuildType({
    name = "> Run :: $suiteId Tests"
    type = Type.COMPOSITE
    id(getId(this::class, name, true))

    dependencies {
        for (tests in testsList) {
            for (test in tests.modules) {
                snapshot(TestsModule(tests.configuration, test)) {}
            }
            if (tests.enableOthers) {
                snapshot(OtherTestsModule(tests.configuration, tests.modules)) {}
            }
        }
    }
})
