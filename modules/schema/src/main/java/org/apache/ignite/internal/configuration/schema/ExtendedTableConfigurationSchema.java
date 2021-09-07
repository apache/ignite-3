package org.apache.ignite.internal.configuration.schema;

import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;
import org.apache.ignite.configuration.validation.Immutable;

@InternalConfiguration
public class ExtendedTableConfigurationSchema extends TableConfigurationSchema {
    @Value
    @Immutable
    public String id;

    @Value(hasDefault = true)
    public byte[] assignments = new byte[0];

    @NamedConfigValue
    public SchemaConfigurationSchema schemas;
}