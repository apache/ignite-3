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

import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;

/**
 * Table storage that contains meta, partitions and SQL indexes.
 */
public interface TableStorage {
    /**
     * Retrieves or creates a partition for current table. Not expected to be called concurrently with the same
     * partition id.
     *
     * @param partId Partition id.
     * @return Partition storage.
     * @throws IllegalArgumentException If partition id is invalid.
     * @throws StorageException If error occurred during partition creation.
     */
    PartitionStorage getOrCreatePartition(int partId) throws StorageException;

    /**
     * Returns partition storage or {@code null} if required storage doesn't exist.
     *
     * @param partId Partition id.
     * @return Partition storage or {@code null}.
     * @throws IllegalArgumentException If partition id is invalid.
     */
    PartitionStorage getPartition(int partId);

    /**
     * Destroys partition if it exists.
     *
     * @param partId Partition id.
     * @throws IllegalArgumentException If partition id is invalid.
     * @throws StorageException If error occurred during partition destruction.
     */
    void dropPartition(int partId) throws StorageException;

    /**
     * Returns table configuration.
     *
     * @return Table configuration.
     */
    TableConfiguration configuration();

    /**
     * Returns data region containing tables data.
     *
     * @return Data region containing tables data.
     */
    DataRegion dataRegion();

    /**
     * Starts the storage.
     *
     * @throws StorageException If something went wrong.
     */
    public void start() throws StorageException;

    /**
     * Stops the storage.
     *
     * @throws StorageException If something went wrong.
     */
    void stop() throws StorageException;

    /**
     * Stops and destroys the storage and cleans all allocated resources.
     *
     * @throws StorageException If something went wrong.
     */
    void destroy() throws StorageException;
}
