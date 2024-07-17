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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.index.IndexNotBuiltException;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class for storages.
 */
public class StorageUtils {
    /**
     * Creates an error message about missing multi-version partition storage.
     *
     * @param partitionId Partition ID.
     */
    public static String createMissingMvPartitionErrorMessage(int partitionId) {
        return "Partition ID " + partitionId + " does not exist";
    }

    /**
     * Throws an exception if the state of the storage is not {@link StorageState#RUNNABLE}.
     *
     * @param state Storage state.
     * @param storageInfoSupplier Storage information supplier, for example in the format "table=user, partitionId=1".
     * @throws StorageClosedException If the storage is closed.
     * @throws StorageRebalanceException If storage is in the process of rebalancing.
     * @throws StorageException For other {@link StorageState}.
     */
    public static void throwExceptionIfStorageNotInRunnableState(StorageState state, Supplier<String> storageInfoSupplier) {
        if (state != StorageState.RUNNABLE) {
            throwExceptionDependingOnStorageState(state, storageInfoSupplier.get());
        }
    }

    /**
     * Throws an exception if the state of the storage is not {@link StorageState#RUNNABLE} OR {@link StorageState#REBALANCE}.
     *
     * @param state Storage state.
     * @param storageInfoSupplier Storage information supplier, for example in the format "table=user, partitionId=1".
     * @throws StorageClosedException If the storage is closed.
     * @throws StorageException For other {@link StorageState}.
     */
    public static void throwExceptionIfStorageNotInRunnableOrRebalanceState(StorageState state, Supplier<String> storageInfoSupplier) {
        if (state != StorageState.RUNNABLE && state != StorageState.REBALANCE) {
            throwExceptionDependingOnStorageState(state, storageInfoSupplier.get());
        }
    }

    /**
     * Throws an exception if the state of the storage is not {@link StorageState#CLEANUP} OR {@link StorageState#REBALANCE}.
     *
     * @param state Storage state.
     * @param storageInfoSupplier Storage information supplier, for example in the format "table=user, partitionId=1".
     * @throws StorageClosedException If the storage is closed.
     * @throws StorageException For other {@link StorageState}.
     */
    public static void throwExceptionIfStorageNotInCleanupOrRebalancedState(StorageState state, Supplier<String> storageInfoSupplier) {
        if (state != StorageState.CLEANUP && state != StorageState.REBALANCE) {
            throwExceptionDependingOnStorageState(state, storageInfoSupplier.get());
        }
    }

    /**
     * Throws an {@link StorageRebalanceException} if the storage is in the process of rebalancing.
     *
     * @param state Storage state.
     * @param storageInfoSupplier Storage information supplier, for example in the format "table=user, partitionId=1".
     */
    public static void throwExceptionIfStorageInProgressOfRebalance(StorageState state, Supplier<String> storageInfoSupplier) {
        if (state == StorageState.REBALANCE) {
            throw new StorageRebalanceException(createStorageInProcessOfRebalanceErrorMessage(storageInfoSupplier.get()));
        }
    }

    /**
     * Throws an {@link StorageRebalanceException} depending on {@link StorageState} on rebalance.
     *
     * @param state Storage state.
     * @param storageInfo Storage information, for example in the format "table=user, partitionId=1".
     * @throws StorageRebalanceException Depending on {@link StorageState}.
     */
    public static void throwExceptionDependingOnStorageStateOnRebalance(StorageState state, String storageInfo) {
        switch (state) {
            case CLOSED:
                throw new StorageRebalanceException(createStorageClosedErrorMessage(storageInfo));
            case REBALANCE:
                throw new StorageRebalanceException(createStorageInProcessOfRebalanceErrorMessage(storageInfo));
            case CLEANUP:
                throw new StorageRebalanceException(createStorageInProcessOfCleanupErrorMessage(storageInfo));
            case DESTROYED:
                throw new StorageRebalanceException(createStorageDestroyedErrorMessage(storageInfo));
            default:
                throw new StorageRebalanceException(createUnexpectedStorageStateErrorMessage(state, storageInfo));
        }
    }

    /**
     * Throws an exception depending on {@link StorageState}.
     *
     * @param state Storage state.
     * @param storageInfo Storage information, for example in the format "table=user, partitionId=1".
     * @throws StorageClosedException If the storage is closed.
     * @throws StorageRebalanceException If storage is in the process of rebalancing.
     * @throws StorageException For other {@link StorageState}.
     */
    public static void throwExceptionDependingOnStorageState(StorageState state, String storageInfo) {
        switch (state) {
            case CLOSED:
                throw new StorageClosedException(createStorageClosedErrorMessage(storageInfo));
            case REBALANCE:
                throw new StorageRebalanceException(createStorageInProcessOfRebalanceErrorMessage(storageInfo));
            case CLEANUP:
                throw new StorageException(createStorageInProcessOfCleanupErrorMessage(storageInfo));
            case DESTROYED:
                throw new StorageDestroyedException(createStorageDestroyedErrorMessage(storageInfo));
            default:
                throw new StorageException(createUnexpectedStorageStateErrorMessage(state, storageInfo));
        }
    }

    /**
     * Throws an exception depending on {@link StorageState}.
     *
     * @param state Storage state.
     * @param read If this is a read.
     * @param storageInfo Storage information, for example in the format "table=user, partitionId=1".
     * @throws StorageClosedException If the storage is closed.
     * @throws StorageRebalanceException If storage is in the process of rebalancing.
     * @throws StorageException For other {@link StorageState}.
     */
    public static void throwExceptionDependingOnIndexStorageState(StorageState state, boolean read, String storageInfo) {
        switch (state) {
            case CLOSED:
                throw new StorageClosedException(createStorageClosedErrorMessage(storageInfo));
            case REBALANCE:
                throw new StorageRebalanceException(createStorageInProcessOfRebalanceErrorMessage(storageInfo));
            case CLEANUP:
                throw new StorageException(createStorageInProcessOfCleanupErrorMessage(storageInfo));
            case DESTROYED:
                if (read) {
                    throw new StorageDestroyedException(IgniteStringFormatter.format(
                            "Read from an index storage that is in the process of being destroyed or already destroyed: [{}]",
                            storageInfo
                    ));
                } else {
                    throw new StorageDestroyedException(createStorageDestroyedErrorMessage(storageInfo));
                }
            default:
                throw new StorageException(createUnexpectedStorageStateErrorMessage(state, storageInfo));
        }
    }

    /**
     * Throws an {@link StorageRebalanceException} if the storage is <strong>NOT</strong> in the process of rebalancing.
     *
     * @param state Storage state.
     * @param storageInfoSupplier Storage information supplier, for example in the format "table=user, partitionId=1".
     */
    public static void throwExceptionIfStorageNotInProgressOfRebalance(StorageState state, Supplier<String> storageInfoSupplier) {
        if (state != StorageState.REBALANCE) {
            throw new StorageRebalanceException(createStorageInProcessOfRebalanceErrorMessage(storageInfoSupplier.get()));
        }
    }

    /**
     * Throws a {@link StorageException} if it is the cause.
     */
    public static void throwStorageExceptionIfItCause(IgniteInternalCheckedException e) {
        if (e.getCause() instanceof StorageException) {
            throw ((StorageException) e.getCause());
        }
    }

    private static String createStorageInProcessOfRebalanceErrorMessage(String storageInfo) {
        return IgniteStringFormatter.format("Storage in the process of rebalancing: [{}]", storageInfo);
    }

    private static String createUnexpectedStorageStateErrorMessage(StorageState state, String storageInfo) {
        return IgniteStringFormatter.format("Unexpected state: [{}, state={}]", storageInfo, state);
    }

    private static String createStorageClosedErrorMessage(String storageInfo) {
        return IgniteStringFormatter.format("Storage is already closed: [{}]", storageInfo);
    }

    private static String createStorageInProcessOfCleanupErrorMessage(String storageInfo) {
        return IgniteStringFormatter.format("Storage is in the process of cleanup: [{}]", storageInfo);
    }

    private static String createStorageDestroyedErrorMessage(String storageInfo) {
        return IgniteStringFormatter.format("Storage is in the process of being destroyed or already destroyed: [{}]", storageInfo);
    }

    /**
     * If not already in a terminal state, transitions to the supplied state and returns {@code true}, otherwise just returns {@code false}.
     */
    public static boolean transitionToTerminalState(StorageState targetState, AtomicReference<StorageState> stateRef) {
        assert targetState.isTerminal() : "Not a terminal state: " + targetState;

        while (true) {
            StorageState previous = stateRef.get();

            if (previous.isTerminal()) {
                return false;
            }

            if (stateRef.compareAndSet(previous, targetState)) {
                return true;
            }
        }
    }

    /**
     * Returns the row ID value used by index storages as the row ID to start the index building process with.
     */
    public static RowId initialRowIdToBuild(int partitionId) {
        return RowId.lowestRowId(partitionId);
    }

    /**
     * Throws an {@link IndexNotBuiltException} if the index has not yet been built.
     *
     * @param nextRowIdToBuild Next row ID to build, {@code null} if the index is built.
     * @param storageInfoSupplier Storage information supplier, for example in the format "indexId=5, partitionId=1".
     */
    public static void throwExceptionIfIndexIsNotBuilt(@Nullable RowId nextRowIdToBuild, Supplier<String> storageInfoSupplier) {
        if (nextRowIdToBuild != null) {
            throw new IndexNotBuiltException("Index not built yet: [{}]", storageInfoSupplier.get());
        }
    }
}
