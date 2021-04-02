package org.apache.ignite.metastorage.configuration;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;

@ConfigurationRoot(rootName = "metastore", storage = TempConfigurationStorage.class)
public class MetastoreManagerConfigurationSchema {
    @Value
    String[] names;
}
