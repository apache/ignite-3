package org.apache.ignite.internal.configuration.storage;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.ConfigurationType;

public class InMemoryConfigurationStorage implements ConfigurationStorage {
    /** Configuration type. */
    private final ConfigurationType configurationType;

    /** Map to store values. */
    private final Map<String, Serializable> map = new HashMap<>();

    /** Change listeners. Guarded by {@code this}. */
    private final Collection<ConfigurationStorageListener> listeners = new ArrayList<>();

    /** Storage version. Guarded by {@code this}. */
    long version = 0;


    /**
     * Constructor.
     *
     * @param type Configuration type.
     * @param data Configuration data.
     */
    public InMemoryConfigurationStorage(ConfigurationType type, Map<String, Serializable> data) {
        configurationType = type;
        map.putAll(data);
    }

    @Override
    public void close() {
        // To reuse this instance with new configuration changer.
        listeners.clear();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<String, ? extends Serializable>> readAllLatest(String prefix) {
        return supplyAsync(() -> {
            synchronized (this) {
                return map.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(prefix))
                        .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Serializable> readLatest(String key) throws StorageException {
        return supplyAsync(() -> {
            synchronized (this) {
                return map.get(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ReadEntry> readDataOnRecovery() {
        return supplyAsync(() -> {
            synchronized (this) {
                return new ReadEntry(new HashMap<>(map), version);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> write(WriteEntry writeEntry) {
        return supplyAsync(() -> {
            synchronized (this) {
                if (writeEntry.version() != version) {
                    return false;
                }

                for (Map.Entry<String, ? extends Serializable> entry : writeEntry.newValues().entrySet()) {
                    if (entry.getValue() != null) {
                        map.put(entry.getKey(), entry.getValue());
                    } else {
                        map.remove(entry.getKey());
                    }
                }

                version++;

                var data = new ReadEntry(writeEntry.newValues(), version);

                listeners.forEach(listener -> listener.onEntriesChanged(data).join());

                return true;
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void registerConfigurationListener(ConfigurationStorageListener listener) {
        listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return configurationType;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Long> lastRevision() {
        return CompletableFuture.completedFuture(version);
    }

    @Override
    public CompletableFuture<Long> localRevision() {
        return lastRevision();
    }

    public void version(long version) {
        this.version = version;
    }
}
