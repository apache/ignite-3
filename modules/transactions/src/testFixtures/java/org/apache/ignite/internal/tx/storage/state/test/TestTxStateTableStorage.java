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
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Table tx state storage for {@link TestTxStateStorage}.
 */
public class TestTxStateTableStorage implements TxStateTableStorage {
    private final Map<Integer, TestTxStateStorageDecorator> storageByPartitionId = new ConcurrentHashMap<>();

    private final Map<Integer, TestTxStateStorage> backupStorageByPartitionId = new ConcurrentHashMap<>();

    private final Map<Integer, CompletableFuture<Void>> startRebalanceFutureByPartitionId = new ConcurrentHashMap<>();

    @Override
    public TxStateStorage getOrCreateTxStateStorage(int partitionId) throws StorageException {
        return storageByPartitionId.computeIfAbsent(partitionId, k -> new TestTxStateStorageDecorator(new TestTxStateStorage()));
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
        TestTxStateStorageDecorator storageDecorator = storageByPartitionId.get(partitionId);

        checkPartitionStoragesExists(storageDecorator, partitionId);

        CompletableFuture<Void> startRebalancePartitionFuture = new CompletableFuture<>();

        if (startRebalanceFutureByPartitionId.putIfAbsent(partitionId, startRebalancePartitionFuture) != null) {
            throw new StorageException("Previous full rebalance did not complete for the partition: " + partitionId);
        }

        try {
            assert !backupStorageByPartitionId.containsKey(partitionId) : partitionId;

            TestTxStateStorage newWriteStorage = new TestTxStateStorage();

            TestTxStateStorage oldWriteStorage = storageDecorator.startRebalance(newWriteStorage);

            backupStorageByPartitionId.put(partitionId, oldWriteStorage);

            startRebalancePartitionFuture.complete(null);
        } catch (Throwable throwable) {
            startRebalanceFutureByPartitionId.remove(partitionId).completeExceptionally(throwable);
        }

        return startRebalancePartitionFuture;
    }

    @Override
    public CompletableFuture<Void> abortRebalance(int partitionId) throws StorageException {
        CompletableFuture<Void> startRebalancePartitionFuture = startRebalanceFutureByPartitionId.get(partitionId);

        if (startRebalancePartitionFuture == null) {
            return completedFuture(null);
        }

        if (!startRebalancePartitionFuture.isDone()) {
            throw new StorageException("Full rebalance for a partition hasn't finished starting yet: " + partitionId);
        }

        CompletableFuture<Void> abortRebalanceFuture = new CompletableFuture<>();

        TestTxStateStorage removedOldWriteStorage = backupStorageByPartitionId.remove(partitionId);

        if (removedOldWriteStorage != null) {
            try {
                TestTxStateStorageDecorator storageDecorator = storageByPartitionId.get(partitionId);

                checkPartitionStoragesExists(storageDecorator, partitionId);

                storageDecorator.abortRebalce(removedOldWriteStorage);

                abortRebalanceFuture.complete(null);
                startRebalanceFutureByPartitionId.remove(partitionId);
            } catch (Throwable throwable) {
                abortRebalanceFuture.completeExceptionally(throwable);
            }
        } else {
            abortRebalanceFuture.complete(null);
        }

        return abortRebalanceFuture;
    }

    @Override
    public CompletableFuture<Void> finishRebalance(int partitionId) throws StorageException {
        CompletableFuture<Void> startRebalancePartitionFuture = startRebalanceFutureByPartitionId.get(partitionId);

        if (startRebalancePartitionFuture == null) {
            return completedFuture(null);
        }

        if (!startRebalancePartitionFuture.isDone()) {
            throw new StorageException("Full rebalance for a partition hasn't finished starting yet: " + partitionId);
        }

        CompletableFuture<Void> finishRebalanceFuture = new CompletableFuture<>();

        TestTxStateStorage removedOldWriteStorage = backupStorageByPartitionId.remove(partitionId);

        if (removedOldWriteStorage != null) {
            try {
                TestTxStateStorageDecorator storageDecorator = storageByPartitionId.get(partitionId);

                checkPartitionStoragesExists(storageDecorator, partitionId);

                storageDecorator.finishRebalance();

                finishRebalanceFuture.complete(null);
                startRebalanceFutureByPartitionId.remove(partitionId);
            } catch (Throwable throwable) {
                finishRebalanceFuture.completeExceptionally(throwable);
            }
        } else {
            finishRebalanceFuture.complete(null);
        }

        return finishRebalanceFuture;
    }

    private void checkPartitionStoragesExists(@Nullable TxStateStorage storages, int partitionId) throws StorageException {
        if (storages == null) {
            throw new StorageException("Partition ID " + partitionId + " does not exist");
        }
    }
}
