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

import java.nio.file.Path;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.StorageException;

/**
 * General storage engine interface.
 */
public interface StorageEngine {
    /**
     * Starts the engine.
     */
    void start();

    /**
     * Stops the engine.
     *
     * @throws StorageException If an error has occurred during the engine stop.
     */
    void stop() throws StorageException;

    /**
     * Creates a new data region.
     *
     * @param regionCfg Data region configuration.
     * @return New data region.
     */
    DataRegion createDataRegion(DataRegionConfiguration regionCfg);

    /**
     * Creates new table storage.
     *
     * @param tablePath              Path to store table data.
     * @param tableCfg               Table configuration.
     * @param dataRegion             Data region for the table.
     * @return New table storage.
     */
    TableStorage createTable(Path tablePath, TableConfiguration tableCfg, DataRegion dataRegion);
}
