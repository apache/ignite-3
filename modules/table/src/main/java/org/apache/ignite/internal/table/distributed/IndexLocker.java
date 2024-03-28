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

package org.apache.ignite.internal.table.distributed;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.Lock;
import org.jetbrains.annotations.Nullable;

/**
 * A decorator interface to hide all tx-protocol-related things.
 *
 * <p>Different indexes requires different approaches for locking. Thus every index type has its own implementation of this interface.
 */
public interface IndexLocker {
    /** Returns an identifier of the index this locker created for. */
    int id();

    /**
     * Acquires the lock for a lookup operation.
     *
     * @param txId Identifier of the transaction in which the row is read.
     * @param key Index key to lookup.
     * @return Future representing the state of the operation.
     */
    CompletableFuture<Void> locksForLookupByKey(UUID txId, BinaryTuple key);

    /**
     * Acquires the lock for a lookup operation.
     *
     * @param txId Identifier of the transaction in which the row is read.
     * @param tableRow Table row to lookup.
     * @return Future representing the state of the operation.
     */
    CompletableFuture<Void> locksForLookup(UUID txId, BinaryRow tableRow);

    /**
     * Acquires the lock for insert operation.
     * If index required a short term lock to insert, the lock is returned as a future result, otherwise future result is {@code null}.
     *
     * @param txId An identifier of the transaction in which the row is inserted.
     * @param tableRow A table row to insert.
     * @param rowId An identifier of the row in the main storage.
     * @return A future representing a result (the result might be {@code null} if no lock has been acquired).
     */
    CompletableFuture<@Nullable Lock> locksForInsert(UUID txId, BinaryRow tableRow, RowId rowId);

    /**
     * Acquires the lock for remove operation.
     *
     * @param txId An identifier of the transaction in which the row is removed.
     * @param tableRow A table row to remove.
     * @param rowId An identifier of the row to remove.
     * @return A future representing a result.
     */
    CompletableFuture<Void> locksForRemove(UUID txId, BinaryRow tableRow, RowId rowId);
}
