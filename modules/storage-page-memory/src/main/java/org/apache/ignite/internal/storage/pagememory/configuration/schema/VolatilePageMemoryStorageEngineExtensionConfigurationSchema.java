package org.apache.ignite.internal.storage.pagememory.configuration.schema;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.internal.storage.configurations.StorageEngineConfigurationSchema;

@ConfigurationExtension
public class VolatilePageMemoryStorageEngineExtensionConfigurationSchema extends StorageEngineConfigurationSchema {

    @ConfigValue
    public VolatilePageMemoryProfileStorageEngineConfigurationSchema aimem;
}
