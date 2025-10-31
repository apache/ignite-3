package test.template_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.build_types.RunSanityCheck

class RunTests(private val tests: Tests, private val suiteId: String = tests.configuration.suiteId) : BuildType({
    name = "> Run :: $suiteId Tests"
    type = Type.COMPOSITE
    id(getId(this::class, name, true))

    dependencies {
        snapshot(RunSanityCheck) {}

        for (test in tests.modules) {
            snapshot(TestsModule(tests.configuration, test)) {}
        }
        if (tests.enableOthers) {
            snapshot(OtherTestsModule(tests.configuration, tests.modules)) {}
        }
    }
})
