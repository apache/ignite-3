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

package org.apache.ignite.internal.tx.storage.state.test;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageDecorator;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageOnRebalance;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Table tx state storage for {@link TestTxStateStorage}.
 */
public class TestTxStateTableStorage implements TxStateTableStorage {
    private final Map<Integer, TxStateStorageDecorator> storageByPartitionId = new ConcurrentHashMap<>();

    private final Map<Integer, TxStateStorage> backupStoragesByPartitionId = new ConcurrentHashMap<>();

    @Override
    public TxStateStorage getOrCreateTxStateStorage(int partitionId) throws StorageException {
        return storageByPartitionId.computeIfAbsent(partitionId, k -> new TxStateStorageDecorator(new TestTxStateStorage()));
    }

    @Override
    public @Nullable TxStateStorage getTxStateStorage(int partitionId) {
        return storageByPartitionId.get(partitionId);
    }

    @Override
    public void destroyTxStateStorage(int partitionId) throws StorageException {
        TxStateStorage storage = storageByPartitionId.remove(partitionId);

        if (storage != null) {
            storage.destroy();
        }
    }

    @Override
    public TableConfiguration configuration() {
        return null;
    }

    @Override
    public void start() throws StorageException {
        // No-op.
    }

    @Override
    public void stop() {
        // No-op.
    }

    @Override
    public void destroy() {
        storageByPartitionId.clear();
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    public CompletableFuture<Void> startRebalance(int partitionId) throws StorageException {
        TxStateStorageDecorator oldStorageDecorator = storageByPartitionId.get(partitionId);

        checkPartitionStoragesExists(oldStorageDecorator, partitionId);

        TxStateStorage oldStorage = oldStorageDecorator.getDelegate();

        TxStateStorage previousOldStorage = backupStoragesByPartitionId.put(partitionId, oldStorage);

        if (previousOldStorage != null) {
            throw new StorageException("Previous full rebalance did not complete for the partition: " + partitionId);
        }

        oldStorageDecorator.replaceDelegate(new TxStateStorageOnRebalance(oldStorage, new TestTxStateStorage()));

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> abortRebalance(int partitionId) {
        TxStateStorage backupStorage = backupStoragesByPartitionId.remove(partitionId);

        if (backupStorage != null) {
            TxStateStorageDecorator storageDecorator = storageByPartitionId.get(partitionId);

            checkPartitionStoragesExists(storageDecorator, partitionId);

            storageDecorator.replaceDelegate(backupStorage);
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> finishRebalance(int partitionId) {
        TxStateStorage backupStorage = backupStoragesByPartitionId.remove(partitionId);

        if (backupStorage != null) {
            TxStateStorageDecorator storageDecorator = storageByPartitionId.get(partitionId);

            checkPartitionStoragesExists(storageDecorator, partitionId);

            TxStateStorageOnRebalance txStateStorageOnRebalance = (TxStateStorageOnRebalance) storageDecorator.getDelegate();

            txStateStorageOnRebalance.finishRebalance();

            storageDecorator.replaceDelegate(txStateStorageOnRebalance.getNewStorage());
        }

        return completedFuture(null);
    }

    private void checkPartitionStoragesExists(@Nullable TxStateStorage storages, int partitionId) throws StorageException {
        if (storages == null) {
            throw new StorageException("Partition ID " + partitionId + " does not exist");
        }
    }
}
