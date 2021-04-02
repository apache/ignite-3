package org.apache.ignite.metastorage.configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;

/**
 * TODO: delete it in the future.
 */
public class TempConfigurationStorage implements ConfigurationStorage {
    /** Map to store values. */
    private Map<String, Serializable> map = new ConcurrentHashMap<>();

    /** Change listeners. */
    private List<ConfigurationStorageListener> listeners = new CopyOnWriteArrayList<>();

    /** Storage version. */
    private AtomicLong version = new AtomicLong(0);

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        return new Data(new HashMap<>(map), version.get(), 0);
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long version) {
        if (version != this.version.get())
            return CompletableFuture.completedFuture(false);

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            if (entry.getValue() != null)
                map.put(entry.getKey(), entry.getValue());
            else
                map.remove(entry.getKey());
        }

        this.version.incrementAndGet();

        listeners.forEach(listener -> listener.onEntriesChanged(new Data(newValues, this.version.get(), 0)));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);
    }

    /** {@inheritDoc} */
    @Override public void notifyApplied(long storageRevision) {
        // No-op.
    }
}
