package org.apache.ignite.internal.rest.configuration;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Test root configuration schema.
 */
@ConfigurationRoot(rootName = "root")
public class TestRootConfigurationSchema {
    /** Foo field. */
    @Value(hasDefault = true)
    public String foo = "foo";

    /** Sub configuration schema. */
    @ConfigValue
    public TestSubConfigurationSchema subCfg;
}
