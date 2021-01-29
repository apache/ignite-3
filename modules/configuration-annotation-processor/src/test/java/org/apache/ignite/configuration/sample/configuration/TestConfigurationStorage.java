package org.apache.ignite.configuration.sample.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;

/**
 * Test configuration storage.
 */
public class TestConfigurationStorage implements ConfigurationStorage {
    /** Fake storage. */
    private final Storage values;

    /** Constructor. */
    public TestConfigurationStorage() {
        this(new Storage());
    }

    /** Constructor. */
    public TestConfigurationStorage(Storage values) {
        this.values = values;
    }

    /** {@inheritDoc} */
    @Override public Data readAll() {
        return new Data(values.getAll(), values.version());
    }

    /** {@inheritDoc} */
    @Override public boolean write(Map<String, Serializable> newValue, int version) {
        return values.putAll(version, newValue);
    }

    /** {@inheritDoc} */
    @Override public Set<String> keys() {
        return values.keySet();
    }

    /** {@inheritDoc} */
    @Override public void addListener(ConfigurationStorageListener listener) {
        this.values.addListener(listener);
    }

    /** {@inheritDoc} */
    @Override public void removeListener(ConfigurationStorageListener listener) {
        this.values.removeListener(listener);
    }

    /**
     *
     */
    public static class Storage {
        /** Map to store values. */
        private Map<String, Serializable> map = new ConcurrentHashMap<>();

        /** Change listeners. */
        private List<ConfigurationStorageListener> listeners = new ArrayList<>();

        /** Storage version. */
        private AtomicInteger version = new AtomicInteger(0);

        /**
         * Get all data from map.
         * @return Data.
         */
        public Map<String, Serializable> getAll() {
            return new HashMap<>(map);
        }

        /**
         * Put all data to map.
         * @param sentVersion Latest known version of the sender.
         * @param newValues New values to put.
         * @return {@code true} if versions match and data was successfully put, {@code false} otherwise.
         */
        public synchronized boolean putAll(int sentVersion, Map<String, Serializable> newValues) {
            if (sentVersion != version.get())
                return false;

            map.putAll(newValues);

            version.incrementAndGet();

            // Make listeners update async, so next storage accessor will have to busy wait for new data to be visible
            new Thread(() ->
                listeners.forEach(listener -> listener.onEntriesChanged(new Data(newValues, version.get())))
            ).start();

            return true;
        }

        /**
         * Add change listener.
         * @param listener Listener.
         */
        public void addListener(ConfigurationStorageListener listener) {
            this.listeners.add(listener);
        }

        /**
         * Remove change listener.
         * @param listener Listener.
         */
        public void removeListener(ConfigurationStorageListener listener) {
            this.listeners.remove(listener);
        }

        /**
         * Get storage version.
         * @return Version.
         */
        public int version() {
            return version.get();
        }

        /**
         * Get all storage keys.
         * @return Keys.
         */
        public Set<String> keySet() {
            return map.keySet();
        }
    }
}
