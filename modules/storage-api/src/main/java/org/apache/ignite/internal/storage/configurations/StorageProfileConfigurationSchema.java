package org.apache.ignite.internal.storage.configurations;

import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;

@PolymorphicConfig
public class StorageProfileConfigurationSchema {

    @PolymorphicId
    public String engineName;

    @InjectedName
    public String name;

}
