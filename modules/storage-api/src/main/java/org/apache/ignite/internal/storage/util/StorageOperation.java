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
interface StorageOperation {
    /**
     * Returns future completion of the operation.
     */
    CompletableFuture<Void> operationFuture();

    /**
     * Return {@code true} if the operation is the final.
     */
    boolean isFinalOperation();

    /**
     * Marks the operation as final.
     */
    void markFinalOperation();

    /**
     * Abstract operation of the storage.
     */
    abstract class AbstractStorageOperation implements StorageOperation {
        private final CompletableFuture<Void> operationFuture = new CompletableFuture<>();

        private volatile boolean finalOperation;

        @Override
        public CompletableFuture<Void> operationFuture() {
            return operationFuture;
        }

        @Override
        public boolean isFinalOperation() {
            return finalOperation;
        }

        @Override
        public void markFinalOperation() {
            finalOperation = true;
        }
    }

    /**
     * Storage creation operation.
     */
    class CreateStorageOperation extends AbstractStorageOperation {
    }

    /**
     * Storage destruction operation.
     */
    class DestroyStorageOperation extends AbstractStorageOperation {
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
        public CompletableFuture<Void> getDestroyFuture() {
            return destroyFuture;
        }
    }

    /**
     * Storage rebalancing start operation.
     */
    class StartRebalanceStorageOperation extends AbstractStorageOperation {
    }

    /**
     * Storage rebalancing abort operation.
     */
    class AbortRebalanceStorageOperation extends AbstractStorageOperation {
    }

    /**
     * Storage rebalancing finish operation.
     */
    class FinishRebalanceStorageOperation extends AbstractStorageOperation {
    }

    /**
     * Storage cleanup operation.
     */
    class CleanupStorageOperation extends AbstractStorageOperation {
    }

    /**
     * Storage close operation.
     */
    class CloseStorageOperation extends AbstractStorageOperation {
    }
}
