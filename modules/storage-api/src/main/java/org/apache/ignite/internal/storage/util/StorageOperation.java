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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;

/**
 * Storage operations.
 */
abstract class StorageOperation {
    private final CompletableFuture<Void> operationFuture = new CompletableFuture<>();

    private volatile boolean finalOperation;

    /**
     * Returns future completion of the operation.
     */
    CompletableFuture<Void> operationFuture() {
        return operationFuture;
    }

    /**
     * Return {@code true} if the operation is the final.
     */
    boolean isFinalOperation() {
        return finalOperation;
    }

    /**
     * Marks the operation as final.
     */
    void markFinalOperation() {
        finalOperation = true;
    }

    /**
     * Creates an error message indicating that the current operation is in progress or closed.
     *
     * @param storageInfo Storage information in the format "table=user, partitionId=1".
     */
    abstract String inProcessErrorMessage(String storageInfo);

    /**
     * Storage creation operation.
     */
    static class CreateStorageOperation extends StorageOperation {
        @Override
        String inProcessErrorMessage(String storageInfo) {
            return "Storage is in process of being created: [" + storageInfo + ']';
        }
    }

    /**
     * Storage destruction operation.
     */
    static class DestroyStorageOperation extends StorageOperation {
        private final CompletableFuture<Void> destroyFuture = new CompletableFuture<>();

        private final AtomicReference<CreateStorageOperation> createStorageOperationReference = new AtomicReference<>();

        /**
         * Attempts to set the storage creation operation.
         *
         * @param createStorageOperation Storage creation operation.
         * @return {@code True} if the operation was set by current method invocation, {@code false} if by another method invocation.
         */
        boolean setCreationOperation(CreateStorageOperation createStorageOperation) {
            return createStorageOperationReference.compareAndSet(null, createStorageOperation);
        }

        /**
         * Returns {@link #setCreationOperation(CreateStorageOperation) set} a storage creation operation.
         */
        @Nullable CreateStorageOperation getCreateStorageOperation() {
            return createStorageOperationReference.get();
        }

        /**
         * Returns the storage destruction future.
         */
        CompletableFuture<Void> getDestroyFuture() {
            return destroyFuture;
        }

        @Override
        String inProcessErrorMessage(String storageInfo) {
            return "Storage is already in process of being destroyed: [" + storageInfo + ']';
        }
    }

    /**
     * Storage rebalancing start operation.
     */
    static class StartRebalanceStorageOperation extends StorageOperation {
        /** Used if the rebalance abortion was called before the rebalance start was completed. */
        private final AtomicReference<AbortRebalanceStorageOperation> abortRebalanceOperation = new AtomicReference<>();

        private final CompletableFuture<Void> startRebalanceFuture = new CompletableFuture<>();

        /**
         * Attempts to set the abort rebalance operation.
         *
         * @param abortRebalanceOperation Abort rebalance operation.
         * @return {@code true} if the operation was set by the current method invocation, {@code false} if by another method invocation.
         */
        boolean setAbortOperation(AbortRebalanceStorageOperation abortRebalanceOperation) {
            return this.abortRebalanceOperation.compareAndSet(null, abortRebalanceOperation);
        }

        /**
         * Returns the {@link #setAbortOperation(AbortRebalanceStorageOperation) set} a abort rebalance operation.
         */
        @Nullable AbortRebalanceStorageOperation getAbortRebalanceOperation() {
            return abortRebalanceOperation.get();
        }

        /**
         * Returns the start rebalance future.
         */
        CompletableFuture<Void> getStartRebalanceFuture() {
            return startRebalanceFuture;
        }

        @Override
        String inProcessErrorMessage(String storageInfo) {
            return "Storage in the process of starting a rebalance: [" + storageInfo + ']';
        }
    }

    /**
     * Storage rebalancing abort operation.
     */
    static class AbortRebalanceStorageOperation extends StorageOperation {
        @Override
        String inProcessErrorMessage(String storageInfo) {
            return "Storage in the process of aborting a rebalance: [" + storageInfo + ']';
        }
    }

    /**
     * Storage rebalancing finish operation.
     */
    static class FinishRebalanceStorageOperation extends StorageOperation {
        @Override
        String inProcessErrorMessage(String storageInfo) {
            return "Storage in the process of finishing a rebalance: [" + storageInfo + ']';
        }
    }

    /**
     * Storage cleanup operation.
     */
    static class CleanupStorageOperation extends StorageOperation {
        @Override
        String inProcessErrorMessage(String storageInfo) {
            return "Storage is in process of being cleaned up: [" + storageInfo + ']';
        }
    }

    /**
     * Storage close operation.
     */
    static class CloseStorageOperation extends StorageOperation {
        @Override
        String inProcessErrorMessage(String storageInfo) {
            return "Storage is in the process of closing: [" + storageInfo + ']';
        }
    }
}
