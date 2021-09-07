package org.apache.ignite.internal.configuration.schema;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;

@Config
public class SchemaConfigurationSchema {
    @Value(hasDefault = true)
    public byte[] schema  = new byte[0];
}
