package org.apache.ignite.internal.rest.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Test sub configuration schema.
 */
@Config
public class TestSubConfigurationSchema {
    /** Bar field. */
    @Value(hasDefault = true)
    public String bar = "bar";
}
