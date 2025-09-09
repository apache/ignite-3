package test.build_types

import jetbrains.buildServer.configs.kotlin.BuildType
import org.apache.ignite.teamcity.Teamcity.Companion.getId
import test.sanity_check.build_types.*


object RunSanityCheck : BuildType({
    id(getId(this::class))
    name = "-> Run :: Sanity Check"
    description = "Run all sanity checks at once"
    type = Type.COMPOSITE

    dependencies {
        snapshot(CodeStyle) {}
        snapshot(Inspections) {}
        snapshot(PMD) {}
        snapshot(LegacyApi) {}
        snapshot(Javadoc) {}
    }
})
