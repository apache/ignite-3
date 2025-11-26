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

import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.IgniteUtils.getTotalMemoryAvailable;

import java.util.Set;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;

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
     * <p>The destruction is not guaranteed to be durable (that is, if a node stops/crashes before persisting this change to disk,
     * the table storage might still be there after node restart).
     *
     * @param tableId Table ID.
     * @throws StorageException If an error has occurs while dropping the table.
     */
    void destroyMvTable(int tableId);

    /**
     * Adds metrics related to the table to the given metric source.
     *
     * @param tableDescriptor Table descriptor.
     * @param metricSource Metric source.
     */
    default void addTableMetrics(StorageTableDescriptor tableDescriptor, StorageEngineTablesMetricSource metricSource) {
        // No-op.
    }

    /**
     * Default size of a data region, maximum between 256 MiB and 20% of the total physical memory.
     *
     * <p>256 MiB, if system was unable to retrieve physical memory size.
     */
    static long defaultDataRegionSize() {
        //noinspection NumericCastThatLosesPrecision
        return Math.max(256 * MiB, (long) (0.2 * getTotalMemoryAvailable()));
    }

    /**
     * Returns IDs of tables for which there are MV partition storages on disk. Those were created and flushed to disk; either
     * destruction was not started for them, or it failed.
     *
     * <p>This method should only be called when the storage is not accessed otherwise (so no storages in it can appear or
     * be destroyed in parallel with this call).
     */
    Set<Integer> tableIdsOnDisk();
}
