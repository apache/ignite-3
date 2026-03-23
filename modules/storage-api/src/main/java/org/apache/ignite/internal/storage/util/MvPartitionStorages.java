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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.util.StorageOperation.AbortRebalanceStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.CleanupStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.CloseStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.CreateStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.DestroyStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.FinishRebalanceStorageOperation;
import org.apache.ignite.internal.storage.util.StorageOperation.StartRebalanceStorageOperation;
import org.jetbrains.annotations.Nullable;

/**
 * Class for storing stores and performing operations on them.
 */
public class MvPartitionStorages<T extends MvPartitionStorage> {
    private final int tableId;

    private final AtomicReferenceArray<T> storageByPartitionId;

    private final ConcurrentMap<Integer, StorageOperation> operationByPartitionId = new ConcurrentHashMap<>();

    private final ConcurrentMap<Integer, CompletableFuture<Void>> rebalanceFutureByPartitionId = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param tableId Table ID.
     * @param partitions Count of partitions.
     */
    public MvPartitionStorages(int tableId, int partitions) {
        this.tableId = tableId;

        storageByPartitionId = new AtomicReferenceArray<>(partitions);
    }

    /**
     * Returns the multi-versioned partition storage, {@code null} if the storage does not exist (not created or destroyed).
     *
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     */
    public @Nullable T get(int partitionId) {
        checkPartitionId(partitionId);

        return storageByPartitionId.get(partitionId);
    }

    /**
     * Creates and adds a new multi-versioned partition storage, if the storage is in the process of being destroyed, it will be recreated
     * after the destruction.
     *
     * @param partitionId Partition ID.
     * @param createStorageFunction Storage creation function, the argument is the partition ID.
     * @return Future of creating a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage already exists or another operation is already in progress.
     * @throws StorageException If the creation of the storage after its destruction is already planned.
     */
    public CompletableFuture<MvPartitionStorage> create(int partitionId, IntFunction<T> createStorageFunction) {
        StorageOperation storageOperation = operationByPartitionId.compute(partitionId, (partId, operation) -> {
            if (operation instanceof DestroyStorageOperation) {
                if (!((DestroyStorageOperation) operation).setCreationOperation(new CreateStorageOperation())) {
                    throw new StorageException(
                            "Creation of the storage after its destruction is already planned: [" + createStorageInfo(partitionId) + ']'
                    );
                }

                return operation;
            }

            if (get(partitionId) != null) {
                throw new StorageException("Storage already exists: [" + createStorageInfo(partitionId) + ']');
            }

            if (operation != null) {
                throwExceptionDependingOnOperation(operation, partitionId);
            }

            return new CreateStorageOperation();
        });

        CompletableFuture<Void> destroyStorageFuture = storageOperation instanceof DestroyStorageOperation
                ? ((DestroyStorageOperation) storageOperation).getDestroyFuture()
                : nullCompletedFuture();

        return destroyStorageFuture.thenApply(unused -> {
            T newStorage = createStorageFunction.apply(partitionId);

            boolean set = storageByPartitionId.compareAndSet(partitionId, null, newStorage);

            assert set : createStorageInfo(partitionId);

            return (MvPartitionStorage) newStorage;
        }).whenComplete((storage, throwable) -> operationByPartitionId.compute(partitionId, (partId, operation) -> {
            assert operation instanceof CreateStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

            return nextOperationIfAvailable(operation);
        }));
    }

    /**
     * Destroys a multi-versioned partition storage.
     *
     * @param partitionId Partition ID.
     * @param destroyStorageFunction Partition destruction function.
     * @return Future of multi-versioned partition storage destruction.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If the storage is in the process of rebalancing.
     */
    public CompletableFuture<Void> destroy(int partitionId, Function<T, CompletableFuture<Void>> destroyStorageFunction) {
        DestroyStorageOperation destroyOp = (DestroyStorageOperation) operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExists(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperation(operation, partitionId);
            }

            return new DestroyStorageOperation();
        });

        T storage = storageByPartitionId.getAndSet(partitionId, null);

        return nullCompletedFuture()
                .thenCompose(unused -> destroyStorageFunction.apply(storage))
                .whenComplete((unused, throwable) -> {
                    operationByPartitionId.compute(partitionId, (partId, operation) -> {
                        assert operation instanceof DestroyStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

                        return nextOperationIfAvailable(operation);
                    });

                    if (throwable == null) {
                        destroyOp.getDestroyFuture().complete(null);
                    } else {
                        destroyOp.getDestroyFuture().completeExceptionally(throwable);
                    }
                });
    }

    /**
     * Clears a multi-versioned partition storage.
     *
     * @param partitionId Partition ID.
     * @param clearStorageFunction Partition clean up function.
     * @return Future of cleaning a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If the storage is in the process of rebalancing.
     */
    public CompletableFuture<Void> clear(int partitionId, Function<T, CompletableFuture<Void>> clearStorageFunction) {
        operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExists(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperation(operation, partitionId);
            }

            return new CleanupStorageOperation();
        });

        return nullCompletedFuture()
                .thenCompose(unused -> clearStorageFunction.apply(get(partitionId)))
                .whenComplete((unused, throwable) ->
                        operationByPartitionId.compute(partitionId, (partId, operation) -> {
                            assert operation instanceof CleanupStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

                            return nextOperationIfAvailable(operation);
                        })
                );
    }

    /**
     * Starts a multi-versioned partition storage rebalance.
     *
     * @param partitionId Partition ID.
     * @param startRebalanceStorageFunction Partition start rebalance function.
     * @return Future of starting rebalance a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If rebalancing is already in progress.
     */
    public CompletableFuture<Void> startRebalance(int partitionId, Function<T, CompletableFuture<Void>> startRebalanceStorageFunction) {
        StartRebalanceStorageOperation startRebalanceOperation = (StartRebalanceStorageOperation) operationByPartitionId.compute(
                partitionId,
                (partId, operation) -> {
                    checkStorageExistsForRebalance(partitionId);

                    if (operation != null) {
                        throwExceptionDependingOnOperationForRebalance(operation, partitionId);
                    }

                    if (rebalanceFutureByPartitionId.containsKey(partitionId)) {
                        throw new StorageRebalanceException(createStorageInProgressOfRebalanceErrorMessage(partitionId));
                    }

                    return new StartRebalanceStorageOperation();
                });

        return nullCompletedFuture()
                .thenCompose(unused -> {
                    CompletableFuture<Void> startRebalanceFuture = startRebalanceStorageFunction.apply(get(partitionId));

                    CompletableFuture<Void> old = rebalanceFutureByPartitionId.put(partitionId, startRebalanceFuture);

                    assert old == null : createStorageInfo(partitionId);

                    return startRebalanceFuture;
                }).whenComplete((unused, throwable) -> {
                    operationByPartitionId.compute(partitionId, (partId, operation) -> {
                        assert operation instanceof StartRebalanceStorageOperation : createStorageInfo(partitionId) + ", op=" + operation;

                        return nextOperationIfAvailable(operation);
                    });

                    // Even if an error occurs, we must be able to abort the rebalance, so we do not report the error.
                    startRebalanceOperation.getStartRebalanceFuture().complete(null);
                });
    }

    /**
     * Aborts a multi-versioned partition storage rebalance if started (successful or not).
     *
     * @param partitionId Partition ID.
     * @param abortRebalanceStorageFunction Partition abort rebalance function.
     * @return Future of aborting rebalance a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist or another operation is already in progress.
     */
    public CompletableFuture<Void> abortRebalance(int partitionId, Function<T, CompletableFuture<Void>> abortRebalanceStorageFunction) {
        StorageOperation storageOperation = operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExistsForRebalance(partitionId);

            if (operation instanceof StartRebalanceStorageOperation) {
                if (!((StartRebalanceStorageOperation) operation).setAbortOperation(new AbortRebalanceStorageOperation())) {
                    throw new StorageRebalanceException("Rebalance abort is already planned: [{}]", createStorageInfo(partitionId));
                }

                return operation;
            }

            if (operation != null) {
                throwExceptionDependingOnOperationForRebalance(operation, partitionId);
            }

            return new AbortRebalanceStorageOperation();
        });

        CompletableFuture<?> startRebalanceFuture = storageOperation instanceof StartRebalanceStorageOperation
                ? ((StartRebalanceStorageOperation) storageOperation).getStartRebalanceFuture() : nullCompletedFuture();

        return startRebalanceFuture
                .thenCompose(unused -> {
                    CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

                    if (rebalanceFuture == null) {
                        return nullCompletedFuture();
                    }

                    return rebalanceFuture
                            .handle((unused1, throwable) -> abortRebalanceStorageFunction.apply(get(partitionId)))
                            .thenCompose(identity());
                }).whenComplete((unused, throwable) ->
                        operationByPartitionId.compute(partitionId, (partId, operation) -> {
                            assert operation instanceof AbortRebalanceStorageOperation :
                                    createStorageInfo(partitionId) + ", op=" + operation;

                            return nextOperationIfAvailable(operation);
                        })
                );
    }

    /**
     * Finishes a successful started multi-versioned partition storage rebalance.
     *
     * @param partitionId Partition ID.
     * @param finishRebalanceStorageFunction Partition finish rebalance function, the argument is the partition ID.
     * @return Future of aborting rebalance a multi-versioned partition storage.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist or another operation is already in progress.
     * @throws StorageRebalanceException If storage rebalancing has not started.
     */
    public CompletableFuture<Void> finishRebalance(int partitionId, Function<T, CompletableFuture<Void>> finishRebalanceStorageFunction) {
        operationByPartitionId.compute(partitionId, (partId, operation) -> {
            checkStorageExistsForRebalance(partitionId);

            if (operation != null) {
                throwExceptionDependingOnOperationForRebalance(operation, partitionId);
            }

            if (!rebalanceFutureByPartitionId.containsKey(partitionId)) {
                throw new StorageRebalanceException("Storage rebalancing did not start: [" + createStorageInfo(partitionId) + ']');
            }

            return new FinishRebalanceStorageOperation();
        });

        return nullCompletedFuture()
                .thenCompose(unused -> {
                    CompletableFuture<Void> rebalanceFuture = rebalanceFutureByPartitionId.remove(partitionId);

                    assert rebalanceFuture != null : createStorageInfo(partitionId);

                    return rebalanceFuture.thenCompose(unused1 -> finishRebalanceStorageFunction.apply(get(partitionId)));
                }).whenComplete((unused, throwable) ->
                        operationByPartitionId.compute(partitionId, (partId, operation) -> {
                            assert operation instanceof FinishRebalanceStorageOperation :
                                    createStorageInfo(partitionId) + ", op=" + operation;

                            return nextOperationIfAvailable(operation);
                        })
                );
    }

    /**
     * Creates a short info of the multi-versioned partition storage in the format "table=user, partitionId=1".
     *
     * @param partitionId Partition ID.
     */
    public String createStorageInfo(int partitionId) {
        return IgniteStringFormatter.format("tableId={}, partitionId={}", tableId, partitionId);
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
                    "Unable to access partition with id outside of configured range: [tableId={}, partitionId={}, partitions={}]",
                    tableId,
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
            throw new StorageException(createStorageDoesNotExistErrorMessage(partitionId));
        }
    }

    /**
     * Checks if the storage exists.
     *
     * @param partitionId Partition ID.
     * @throws IllegalArgumentException If partition ID is out of configured bounds.
     * @throws StorageRebalanceException If the storage does not exist.
     */
    private void checkStorageExistsForRebalance(int partitionId) {
        if (get(partitionId) == null) {
            throw new StorageRebalanceException(createStorageDoesNotExistErrorMessage(partitionId));
        }
    }

    private void throwExceptionDependingOnOperation(StorageOperation operation, int partitionId) {
        throw new StorageException(operation.inProcessErrorMessage(createStorageInfo(partitionId)));
    }

    private void throwExceptionDependingOnOperationForRebalance(StorageOperation operation, int partitionId) {
        throw new StorageRebalanceException(operation.inProcessErrorMessage(createStorageInfo(partitionId)));
    }

    private String createStorageDoesNotExistErrorMessage(int partitionId) {
        return "Storage does not exist: [" + createStorageInfo(partitionId) + ']';
    }

    private String createStorageInProgressOfRebalanceErrorMessage(int partitionId) {
        return "Storage in the process of rebalance: [" + createStorageInfo(partitionId) + ']';
    }

    private static @Nullable StorageOperation nextOperationIfAvailable(StorageOperation operation) {
        operation.operationFuture().complete(null);

        if (operation.isFinalOperation()) {
            return operation;
        }

        if (operation instanceof DestroyStorageOperation) {
            return ((DestroyStorageOperation) operation).getCreateStorageOperation();
        }

        if (operation instanceof StartRebalanceStorageOperation) {
            return ((StartRebalanceStorageOperation) operation).getAbortRebalanceOperation();
        }

        return null;
    }

    /**
     * Returns a list of all existing storages.
     *
     * <p>Note: this method may produce races when a rebalance is happening concurrently as the underlying storage array may change.
     * The callers of this method should resolve these races themselves.
     */
    public List<T> getAll() {
        var list = new ArrayList<T>(storageByPartitionId.length());

        for (int i = 0; i < storageByPartitionId.length(); i++) {
            T storage = storageByPartitionId.get(i);

            if (storage != null) {
                list.add(storage);
            }
        }

        return list;
    }

    /**
     * Returns a stream of all existing storages.
     *
     * <p>Note: this method may produce races when a rebalance is happening concurrently as the underlying storage array may change.
     * The callers of this method should resolve these races themselves.
     */
    public Stream<T> stream() {
        return IntStream.range(0, storageByPartitionId.length())
                .mapToObj(storageByPartitionId::get)
                .filter(Objects::nonNull);
    }

    /**
     * Returns all storages for closing or destroying after completion of operations for all storages.
     *
     * <p>After completing the future, when try to perform any operation, {@link StorageException} for all storages will be thrown.
     *
     * @return Future that at the complete will return all the storages that are not destroyed.
     */
    public CompletableFuture<List<T>> getAllForCloseOrDestroy() {
        List<CompletableFuture<Void>> operationFutures = new ArrayList<>();

        for (int partitionId = 0; partitionId < storageByPartitionId.length(); partitionId++) {
            StorageOperation storageOperation = operationByPartitionId.compute(partitionId, (partId, operation) -> {
                if (operation == null) {
                    operation = new CloseStorageOperation();
                }

                operation.markFinalOperation();

                return operation;
            });

            if (!(storageOperation instanceof CloseStorageOperation)) {
                operationFutures.add(storageOperation.operationFuture());
            }
        }

        return CompletableFuture.allOf(operationFutures.toArray(CompletableFuture[]::new))
                .thenApply(unused ->
                        IntStream.range(0, storageByPartitionId.length())
                                .mapToObj(partitionId -> storageByPartitionId.getAndSet(partitionId, null))
                                .filter(Objects::nonNull)
                                .collect(toList())
                );
    }
}
