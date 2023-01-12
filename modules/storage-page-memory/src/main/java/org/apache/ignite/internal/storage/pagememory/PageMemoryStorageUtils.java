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

package org.apache.ignite.internal.storage.pagememory;

import java.util.function.Supplier;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * Helper class for {@link PageMemory}-based storages.
 */
public class PageMemoryStorageUtils {
    /**
     * Runs a function under a busyLock, if it was not possible to acquire(busy) busyLock throws an exception depending on
     * {@link StorageState}.
     *
     * @param <V> Type of the returned value.
     * @param busyLock Busy lock.
     * @param supplier Function.
     * @param storageInfoSupplier Storage state supplier.
     * @param storageStateSupplier Storage information supplier, for example in the format "table=user, partitionId=1".
     * @return Value.
     * @throws StorageClosedException If the storage is closed.
     * @throws StorageRebalanceException If storage is in the process of rebalancing.
     * @throws StorageException For other {@link StorageState}.
     */
    public static <V> V inBusyLock(
            IgniteSpinBusyLock busyLock,
            Supplier<V> supplier,
            Supplier<StorageState> storageStateSupplier,
            Supplier<String> storageInfoSupplier
    ) {
        if (!busyLock.enterBusy()) {
            StorageState state = storageStateSupplier.get();

            switch (state) {
                case CLOSED:
                    throw new StorageClosedException(createStorageClosedErrorMessage(storageInfoSupplier.get()));
                case REBALANCE:
                    throw new StorageRebalanceException(createStorageClosedErrorMessage(storageInfoSupplier.get()));
                default:
                    throw new StorageException(createUnexpectedStorageStateErrorMessage(state, storageInfoSupplier.get()));
            }
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
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
            default:
                throw new StorageRebalanceException(createUnexpectedStorageStateErrorMessage(state, storageInfo));
        }
    }

    private static String createStorageClosedErrorMessage(String storageInfo) {
        return IgniteStringFormatter.format("Storage is already closed: [{}]", storageInfo);
    }

    private static String createStorageInProcessOfRebalanceErrorMessage(String storageInfo) {
        return IgniteStringFormatter.format("Storage in the process of rebalancing: [{}]", storageInfo);
    }

    private static String createUnexpectedStorageStateErrorMessage(StorageState state, String storageInfo) {
        return IgniteStringFormatter.format("Unexpected state: [{}, state={}]", storageInfo, state);
    }
}
