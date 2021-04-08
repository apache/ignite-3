package org.apache.ignite.configuration.schemas.table;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.schemas.TempConfigurationStorage;

@ConfigurationRoot(rootName = "table", storage = TempConfigurationStorage.class)
public class TablesConfigurationSchema {

    @NamedConfigValue
    TableConfigurationSchema tables;
}
