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

package org.apache.ignite.internal.storage.engine;

import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.store.DataStorageChange;
import org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.DataStorageView;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageView;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfigurationSchema;
import org.apache.ignite.internal.storage.StorageException;

/**
 * General storage engine interface.
 */
public interface StorageEngine {
    /**
     * Returns the unique name of the storage engine.
     *
     * <p>Used to map {@link DataStorageConfigurationSchema#name} to {@link StorageEngine#name}.
     */
    String name();

    /**
     * Starts the engine.
     *
     * @throws StorageException If an error has occurred during the engine start.
     */
    void start() throws StorageException;

    /**
     * Stops the engine.
     *
     * @throws StorageException If an error has occurred during the engine stop.
     */
    void stop() throws StorageException;

    /**
     * Creates new table storage.
     *
     * @param tableCfg Table configuration.
     * @throws StorageException If an error has occurs while creating the table.
     */
    TableStorage createTable(TableConfiguration tableCfg) throws StorageException;

    /**
     * Returns a consumer that will set the default {@link TableConfigurationSchema#dataStorage table data storage}.
     *
     * @param defaultDataStorageView View of the {@link TablesConfigurationSchema#defaultDataStorage}. Either {@link UnknownDataStorageView}
     *      or a view of {@link DataStorageConfigurationSchema} extension of the current engine, for example {@code
     *      ConcurrentHashMapDataStorageView}.
     */
    Consumer<DataStorageChange> defaultTableDataStorageConsumer(DataStorageView defaultDataStorageView);
}
