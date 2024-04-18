/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;

/**
 * General storage engine interface.
 */
public interface StorageEngine {
    /**
     * Returns a storage engine name.
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
     * Whether the data is lost upon engine restart or not.
     */
    boolean isVolatile();

    /**
     * Creates new table storage.
     *
     * @param tableDescriptor Table descriptor.
     * @param indexDescriptorSupplier Index descriptor supplier at table start.
     * @throws StorageException If an error has occurs while creating the table.
     */
    // TODO: IGNITE-19717 Get rid of indexDescriptorSupplier
    MvTableStorage createMvTable(StorageTableDescriptor tableDescriptor, StorageIndexDescriptorSupplier indexDescriptorSupplier);

    /**
     * Destroys the table if it exists.
     *
     * @param tableId Table ID.
     * @throws StorageException If an error has occurs while dropping the table.
     */
    void dropMvTable(int tableId);
}
