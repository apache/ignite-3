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

package org.apache.ignite.internal.storage.chm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of the {@link TableStorage} based on class {@link ConcurrentHashMap}.
 */
public class TestConcurrentHashMapTableStorage implements TableStorage {
    private final TableConfiguration tableConfig;

    private final Map<Integer, TestConcurrentHashMapPartitionStorage> partitions = new ConcurrentHashMap<>();

    private volatile boolean started;

    /**
     * Constructor.
     *
     * @param tableConfig Table configuration.
     */
    public TestConcurrentHashMapTableStorage(TableConfiguration tableConfig) {
        this.tableConfig = tableConfig;
    }

    /** {@inheritDoc} */
    @Override
    public PartitionStorage getOrCreatePartition(int partId) throws StorageException {
        assert partId >= 0 : partId;
        assert started;

        return partitions.computeIfAbsent(partId, TestConcurrentHashMapPartitionStorage::new);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionStorage getPartition(int partId) {
        assert partId >= 0 : partId;
        assert started;

        return partitions.get(partId);
    }

    /** {@inheritDoc} */
    @Override
    public void dropPartition(int partId) throws StorageException {
        PartitionStorage partitionStorage = getPartition(partId);

        if (partitionStorage != null) {
            partitionStorage.destroy();
        }
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public TableConfiguration configuration() {
        return tableConfig;
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        started = true;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        destroy();
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        started = false;

        partitions.values().forEach(TestConcurrentHashMapPartitionStorage::destroy);
    }
}
