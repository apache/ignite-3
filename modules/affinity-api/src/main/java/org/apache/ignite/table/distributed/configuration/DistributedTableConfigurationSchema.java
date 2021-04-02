package org.apache.ignite.table.distributed.configuration;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;

@ConfigurationRoot(rootName = "table", storage = TempConfigurationStorage.class)
public class DistributedTableConfigurationSchema {

    @NamedConfigValue
    TableConfigurationSchema tables;
}
