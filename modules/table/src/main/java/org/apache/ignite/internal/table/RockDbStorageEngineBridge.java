/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Internal table facade provides low-level methods for table operations. The facade hides TX/replication protocol over table storage
 * abstractions.
 */
public class RockDbStorageEngineBridge implements StorageEngineBridge {
    private final TableManager tableManager;

    public RockDbStorageEngineBridge(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    @Override
    public CompletableFuture<BinaryRow> get(IgniteUuid tableId, BinaryRow keyRow, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.get(keyRow, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Collection<BinaryRow>> getAll(IgniteUuid tableId, Collection<BinaryRow> keyRows,
            @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.getAll(keyRows, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> upsert(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.upsert(row, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> upsertAll(IgniteUuid tableId, Collection<BinaryRow> rows, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.upsertAll(rows, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.getAndUpsert(row, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> insert(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.insert(row, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Collection<BinaryRow>> insertAll(IgniteUuid tableId, Collection<BinaryRow> rows,
            @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.insertAll(rows, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> replace(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.replace(row, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> replace(IgniteUuid tableId, BinaryRow oldRow, BinaryRow newRow, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.replace(oldRow, newRow, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BinaryRow> getAndReplace(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.getAndReplace(row, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> delete(IgniteUuid tableId, BinaryRow keyRow, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.delete(keyRow, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> deleteExact(IgniteUuid tableId, BinaryRow oldRow, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.deleteExact(oldRow, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BinaryRow> getAndDelete(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.getAndDelete(row, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAll(IgniteUuid tableId, Collection<BinaryRow> rows,
            @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.deleteAll(rows, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAllExact(IgniteUuid tableId, Collection<BinaryRow> rows,
            @Nullable InternalTransaction tx) {
        try {
            return tableManager.tableAsync(tableId)
                    .thenApply(TableImpl::internalTable)
                    .thenCompose(t -> t.deleteAllExact(rows, tx));
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public @NotNull Publisher<BinaryRow> scan(IgniteUuid tableId, int p,
            @Nullable InternalTransaction tx, @Nullable ScanContext scanContext) {
        assert scanContext == null;

        try {
            return tableManager.table(tableId).internalTable().scan(p, tx);
        } catch (NodeStoppingException e) {
            return subscriber -> subscriber.onError(e);
        }
    }
}
