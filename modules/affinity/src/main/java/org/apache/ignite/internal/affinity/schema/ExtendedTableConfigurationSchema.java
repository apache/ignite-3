package org.apache.ignite.internal.affinity.schema;

import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;

@InternalConfiguration
public class ExtendedTableConfigurationSchema extends TableConfigurationSchema {
//    @Value(hasDefault = true)
    public byte[] assignments;// = new byte[0];
}
