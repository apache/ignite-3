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

package org.apache.ignite.internal.table.impl;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Dummy table storage implementation.
 * TODO asch rename to local table
 */
public class DummyInternalTableImpl implements InternalTable {
    /** In-memory dummy store. */
    private final VersionedRowStore store;

    /** TX manager. */
    private final TxManager txManager;

    /** */
    private final UUID tableId = UUID.randomUUID();

    /**
     * @param store The store.
     * @param txManager Transaction manager.
     */
    public DummyInternalTableImpl(VersionedRowStore store, TxManager txManager) {
        this.store = store;
        this.txManager = txManager;
    }

    /** {@inheritDoc} */
    @Override public @NotNull UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull SchemaMode schemaMode() {
        return SchemaMode.STRICT_SCHEMA;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaMode schemaMode) {
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        // TODO asch get rid of copy paste.
        if (tx == null) {
            try {
                tx = txManager.tx();
            }
            catch (TransactionException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (tx != null) {
            InternalTransaction finalTx = tx;
            return txManager.readLock(tableId, row.keySlice(), tx.timestamp()).thenApply(ignore -> store.get(row, finalTx.timestamp()));
        }
        else {
            InternalTransaction tx0 = txManager.begin();

            // TODO asch lock doesn't look necessary for single key read.
            return txManager.readLock(tableId, row.keySlice(), tx0.timestamp()).
                thenApply(ignore -> store.get(row, tx0.timestamp())).
                thenCompose(r -> tx0.commitAsync().thenApply(ignored -> r));
        }
    }

    /**
     * @param row The row.
     * @param tx The transaction.
     * @param op The operation.
     * @return The future.
     */
    private <T> CompletableFuture<T> wrapInTx(@NotNull BinaryRow row, InternalTransaction tx, Function<InternalTransaction, T> op) {
        assert row != null;

        if (tx == null) {
            try {
                tx = txManager.tx();
            }
            catch (TransactionException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (tx != null) {
            InternalTransaction finalTx = tx;

            return txManager.writeLock(tableId, row.keySlice(), tx.timestamp()).
                thenApply(ignore -> op.apply(finalTx));
        }
        else {
            InternalTransaction tx0 = txManager.begin();

            return txManager.writeLock(tableId, row.keySlice(), tx0.timestamp()).
                thenApply(ignore -> op.apply(tx0)).
                thenCompose(r -> tx0.commitAsync().thenApply(ignore -> r));
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(@NotNull BinaryRow row, InternalTransaction tx) {
        return wrapInTx(row, tx, tx0 -> {
            store.upsert(row, tx0.timestamp());

            return null;
        });
    }

    /**
     * TODO asch refactor to utility method.
     * Extracts a key from the {@link BinaryRow} and wraps it in a {@link SearchRow}.
     *
     * @param row Binary row.
     * @return Search row.
     */
    @NotNull private static byte[] extractAndWrapKey(@NotNull BinaryRow row) {
        // TODO asch can reuse thread local byte buffer
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return key;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(@NotNull BinaryRow row,
        InternalTransaction tx) {
        assert row != null;

        return completedFuture(store.getAndUpsert(row, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return wrapInTx(row, tx, tx0 -> store.delete(row, tx0.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows,
        InternalTransaction tx) {
        assert keyRows != null && !keyRows.isEmpty();

        return completedFuture(store.getAll(keyRows, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        store.upsertAll(rows, tx.timestamp());

        return completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return wrapInTx(row, tx, tx0 -> store.insert(row, tx0.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        return completedFuture(store.insertAll(rows, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return completedFuture(store.replace(row, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, InternalTransaction tx) {
        assert oldRow != null;
        assert newRow != null;

        return completedFuture(store.replace(oldRow, newRow, tx.timestamp()));
    }

    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow row, InternalTransaction tx) {
        assert row != null;
        assert row.hasValue();

        return completedFuture(store.deleteExact(row, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, InternalTransaction tx) {
        return completedFuture(store.getAndDelete(row, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows,
        InternalTransaction tx) {
        return completedFuture(store.deleteAll(rows, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        InternalTransaction tx) {
        return completedFuture(store.deleteAllExact(rows, tx.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public int partition(BinaryRow keyRow) {
        return 0;
    }
}
