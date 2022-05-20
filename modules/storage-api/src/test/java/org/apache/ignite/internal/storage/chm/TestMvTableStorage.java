/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;

/**
 * Test table storage implementation.
 */
public class TestMvTableStorage implements MvTableStorage {
    private final TableConfiguration tableConfig;

    private final Map<Integer, TestMvPartitionStorage> partitions = new ConcurrentHashMap<>();

    public TestMvTableStorage(TableConfiguration tableCfg) {
        this.tableConfig = tableCfg;
    }

    @Override
    public MvPartitionStorage createPartition(int partitionId) throws StorageException {
        partitions.put(partitionId, new TestMvPartitionStorage(List.of(), partitionId));

        return partition(partitionId);
    }

    @Override
    public MvPartitionStorage partition(int partitionId) {
        return Objects.requireNonNull(partitions.get(partitionId), "Partition doesn't exist");
    }

    @Override
    public CompletableFuture<?> destroyPartition(int partitionId) throws StorageException {
        partitions.remove(partitionId);

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public TableConfiguration configuration() {
        return tableConfig;
    }

    @Override
    public void start() throws StorageException {
    }

    @Override
    public void stop() throws StorageException {
    }

    @Override
    public void destroy() throws StorageException {
    }
}
