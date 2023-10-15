package org.apache.ignite.internal.storage.configurations;

import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;

@PolymorphicConfig
public class StorageEngineConfigurationSchema {

    @PolymorphicId
    public String engineName;
}
