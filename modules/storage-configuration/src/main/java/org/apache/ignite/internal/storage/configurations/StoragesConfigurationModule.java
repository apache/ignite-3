package org.apache.ignite.internal.storage.configurations;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;

@AutoService(ConfigurationModule.class)
public class StoragesConfigurationModule implements ConfigurationModule {
    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<RootKey<?, ?>> rootKeys() {
        return List.of(StoragesConfiguration.KEY);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of(
                DummyStorageEngineConfigurationSchema.class,
                DummyStorageProfileConfigurationSchema.class);
    }
}
