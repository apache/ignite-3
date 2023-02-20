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

package org.apache.ignite.internal.storage.util;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntFunction;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.util.StorageOperation.CreateStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.DestroyStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.RebalanceStorageOperation;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Class for storing stores and performing operations on them.
 */
public class MvPartitionStorages {
    private final TableView tableView;

    private final AtomicReferenceArray<MvPartitionStorage> storageByPartitionId;

    private final ConcurrentMap<Integer, StorageOperation> operationByPartitionId = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param tableView Table configuration.
     */
    public MvPartitionStorages(TableView tableView) {
        this.tableView = tableView;

        storageByPartitionId = new AtomicReferenceArray<>(tableView.partitions());
    }

    /**
     * Returns the multi-versioned partition storage, {@code null} if the storage does not exist (not created or destroyed).
     *
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     */
    public @Nullable MvPartitionStorage get(int partitionId) {
        checkPartitionId(partitionId);

        return storageByPartitionId.get(partitionId);
    }

    /**
     * Creates and adds a new multi-versioned partition storage, if the storage is in the process of being destroyed, it will be
     * recreated after the destruction.
     *
     * @param partitionId Partition ID.
     * @param createStorageFunction Storage creation function, the argument is the partition ID.
     * @return Future of creating a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage already exists or is in the process of being created.
     * @throws StorageException If the creation of the storage after its destruction is already planned.
     */
    public CompletableFuture<MvPartitionStorage> create(int partitionId, IntFunction<MvPartitionStorage> createStorageFunction) {
        StorageOperation storageOperation = operationByPartitionId.compute(partitionId, (partId, operation) -> {
            if (get(partitionId) != null || operation instanceof CreateStorageOperation) {
                throw new StorageException("Storage already exists or is being created: [" + createStorageInfo(partitionId) + ']');
            }

            CreateStorageOperation createStorageOperation = new CreateStorageOperation();

            if (operation == null) {
                return createStorageOperation;
            }

            assert operation instanceof DestroyStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

            if (!((DestroyStorageOperation) operation).setCreationOperation(createStorageOperation)) {
                throw new StorageException(
                        "Creation of the storage after its destruction is already planned: [" + createStorageInfo(partitionId) + ']'
                );
            }

            return operation;
        });

        CompletableFuture<Void> destroyStorageFuture = storageOperation instanceof DestroyStorageOperation
                ? ((DestroyStorageOperation) storageOperation).getDestroyFuture()
                : completedFuture(null);

        return destroyStorageFuture.thenApply(unused -> {
            MvPartitionStorage newStorage = createStorageFunction.apply(partitionId);

            boolean set = storageByPartitionId.compareAndSet(partitionId, null, newStorage);

            assert set : createStorageInfo(partitionId);

            return newStorage;
        }).whenComplete((storage, throwable) -> operationByPartitionId.compute(partitionId, (partId, operation) -> {
            assert operation instanceof CreateStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

            return null;
        }));
    }

    /**
     * Destroys the multi-versioned partition storage.
     *
     * @param partitionId Partition ID.
     * @param destroyStorageFunction Partition destruction function, the argument is the partition ID.
     * @return Future of multi-versioned partition storage destruction.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage does not exist or is in the process of being created or destroyed.
     * @throws StorageRebalanceException If the storage is in the process of rebalancing.
     */
    public CompletableFuture<Void> destroy(int partitionId, IntFunction<CompletableFuture<Void>> destroyStorageFunction) {
        DestroyStorageOperation destroyOp = (DestroyStorageOperation) operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExists(partitionId);

            if (operation instanceof CreateStorageOperation) {
                throw new StorageException("Storage is in process of being created: [" + createStorageInfo(partitionId) + ']');
            } else if (operation instanceof DestroyStorageOperation) {
                throw new StorageException("Storage is already in process of being destroyed: [" + createStorageInfo(partitionId) + ']');
            } else if (operation instanceof RebalanceStorageOperation) {
                throw new StorageRebalanceException("Storage is in process of being rebalanced: [" + createStorageInfo(partitionId) + ']');
            }

            assert operation == null : createStorageInfo(partitionId) + ", op=" + operation;

            return new DestroyStorageOperation();
        });

        return completedFuture(null)
                .thenCompose(unused -> destroyStorageFunction.apply(partitionId))
                .thenAccept(unused -> storageByPartitionId.set(partitionId, null))
                .whenComplete((unused, throwable) -> {
                    operationByPartitionId.compute(partitionId, (partId, operation) -> {
                        assert operation instanceof DestroyStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

                        DestroyStorageOperation destroyStorageOperation = (DestroyStorageOperation) operation;

                        return destroyStorageOperation.getCreateStorageOperation();
                    });

                    if (throwable == null) {
                        destroyOp.getDestroyFuture().complete(null);
                    } else {
                        destroyOp.getDestroyFuture().completeExceptionally(throwable);
                    }
                });
    }

    /**
     * Returns table name.
     */
    public String getTableName() {
        return tableView.name();
    }

    /**
     * Creates a short info of the multi-versioned partition storage in the format "table=user, partitionId=1".
     *
     * @param partitionId Partition ID.
     */
    public String createStorageInfo(int partitionId) {
        return IgniteStringFormatter.format("table={}, partitionId={}", getTableName(), partitionId);
    }

    /**
     * Checks that the partition ID is within the scope of the configuration.
     *
     * @param partitionId Partition ID.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     */
    private void checkPartitionId(int partitionId) {
        int partitions = storageByPartitionId.length();

        if (partitionId < 0 || partitionId >= partitions) {
            throw new IllegalArgumentException(IgniteStringFormatter.format(
                    "Unable to access partition with id outside of configured range: [table={}, partitionId={}, partitions={}]",
                    getTableName(),
                    partitionId,
                    partitions
            ));
        }
    }

    /**
     * Checks if the storage exists.
     *
     * @param partitionId Partition ID.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage does not exist.
     */
    private void checkStorageExists(int partitionId) {
        if (get(partitionId) == null) {
            throw new StorageException("Storage does not exist: [" + createStorageInfo(partitionId) + ']');
        }
    }
}
