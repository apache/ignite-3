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

package org.apache.ignite.internal.table.distributed.gc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;

/**
 * Container for handling storage by the garbage collector.
 */
class GcStorageHandler {
    /**
     * Handler of multi-versioned partition storage and its indexes for garbage collection.
     *
     * @see StorageUpdateHandler#vacuum(HybridTimestamp)
     */
    final StorageUpdateHandler storageUpdateHandler;

    /**
     * Reference to the future of garbage collection batch for multi-versioned partition storage and its indexes.
     *
     * <p>Before the garbage collection batch begins, the new future must be inserted, after the garbage collection batch ends, the
     * inserted future must be completed and removed.
     */
    final AtomicReference<CompletableFuture<Void>> gcInProgressFuture = new AtomicReference<>();

    GcStorageHandler(StorageUpdateHandler storageUpdateHandler) {
        this.storageUpdateHandler = storageUpdateHandler;
    }
}
