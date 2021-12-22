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

package org.apache.ignite.internal.configuration.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.jetbrains.annotations.NotNull;

/**
 * Common interface for configuration storage.
 */
public interface ConfigurationStorage {
    /**
     * Read all configuration values and current storage version.
     *
     * @return Values and version.
     * @throws StorageException If failed to retrieve data.
     */
    Data readAll() throws StorageException;

    /**
     * Retrieves the most recent values which keys start with the given prefix, regardless of the current storage version.
     *
     * @param prefix Key prefix.
     * @return Storage data (keys and values).
     * @throws StorageException If failed to retrieve data.
     */
    Map<String, ? extends Serializable> readAllLatest(String prefix) throws StorageException;

    /**
     * Retrieves the most recent value associated with the key, regardless of the current storage version.
     *
     * @param key Key.
     * @return Value from the storage.
     * @throws StorageException If failed to retrieve data.
     */
    Serializable readLatest(String key) throws StorageException;

    /**
     * Write key-value pairs into the storage with last known version.
     *
     * @param newValues Key-value pairs.
     * @param ver       Last known version.
     * @return Future that gives you {@code true} if successfully written, {@code false} if version of the storage is different from the
     *      passed argument and {@link StorageException} if failed to write data.
     */
    CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long ver);

    /**
     * Add listener to the storage that notifies of data changes.
     *
     * @param lsnr Listener. Cannot be null.
     */
    void registerConfigurationListener(@NotNull ConfigurationStorageListener lsnr);

    /**
     * Returns type of this configuration storage.
     *
     * @return Type of this configuration storage.
     */
    ConfigurationType type();

    /**
     * Returns a future that will be completed when the latest revision of the storage is received.
     */
    CompletableFuture<Long> lastRevision();
}
