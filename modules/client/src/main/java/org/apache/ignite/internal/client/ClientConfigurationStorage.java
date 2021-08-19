package org.apache.ignite.internal.client;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.jetbrains.annotations.NotNull;

public class ClientConfigurationStorage implements ConfigurationStorage {
    /** {@inheritDoc} */
    @Override public Data readAll() throws StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long ver) {
        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public void registerConfigurationListener(@NotNull ConfigurationStorageListener lsnr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }
}
