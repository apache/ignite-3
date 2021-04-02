package org.apache.ignite.table.distributed.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;

@Config
public class TableConfigurationSchema {
    @Value(immutable = true)
    public String name;

    @Value
    public int partitions;

    @Value
    public int backups;
}
