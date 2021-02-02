/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.ignite.configuration.storage.StorageException;

/**
 * Test configuration storage.
 */
public class TestConfigurationStorage implements ConfigurationStorage {
    /** Fake storage. */
    private final Storage values;

    /** Should fail on every operation. */
    private boolean fail = false;

    /** Constructor. */
    public TestConfigurationStorage() {
        this(new Storage());
    }

    /** Constructor. */
    public TestConfigurationStorage(Storage values) {
        this.values = values;
    }

    /**
     * Set fail flag.
     * @param fail Fail flag.
     */
    public void fail(boolean fail) {
        this.fail = fail;
    }

    /** {@inheritDoc} */
    @Override public Data readAll() throws StorageException {
        if (fail)
            throw new StorageException("Failed to read data");

        return new Data(values.getAll(), values.version());
    }

    /** {@inheritDoc} */
    @Override public boolean write(Map<String, Serializable> newValue, int version) throws StorageException {
        if (fail)
            throw new StorageException("Failed to write data");

        return values.putAll(version, newValue);
    }

    /** {@inheritDoc} */
    @Override public Set<String> keys() throws StorageException {
        if (fail)
            throw new StorageException("Failed to get keys");

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
