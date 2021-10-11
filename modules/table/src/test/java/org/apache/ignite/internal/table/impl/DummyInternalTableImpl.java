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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Function;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.schema.definition.SchemaManagementMode;
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
    private final IgniteUuid tableId = new IgniteUuidGenerator(UUID.randomUUID(), 0).randomUuid();

    /**
     * @param store The store.
     * @param txManager Transaction manager.
     */
    public DummyInternalTableImpl(VersionedRowStore store, TxManager txManager) {
        this.store = store;
        this.txManager = txManager;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteUuid tableId() {
        return new IgniteUuid(UUID.randomUUID(), 0);
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull SchemaManagementMode schemaMode() {
        return SchemaManagementMode.STRICT;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaManagementMode schemaMode) {
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

    /**
     * @param row The row.
     * @param tx The transaction.
     * @param op The operation.
     * @return The future.
     */
    private <T> CompletableFuture<T> wrapInTx(
        @NotNull Collection<BinaryRow> rows,
        InternalTransaction tx,
        Function<InternalTransaction, T> op
    ) {
        assert rows != null;

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

            // TODO asch copypaste.
            CompletableFuture<Void>[] futs = new CompletableFuture[rows.size()];

            int i = 0;

            for (BinaryRow row : rows)
                futs[i++] = txManager.writeLock(tableId, row.keySlice(), tx.timestamp());

            // If some lock has failed to acquire, tx will be rolled back and will release acquired locks.
            return CompletableFuture.allOf(futs).thenApply(ignore -> op.apply(finalTx));
        }
        else {
            InternalTransaction tx0 = txManager.begin();

            CompletableFuture<Void>[] futs = new CompletableFuture[rows.size()];

            int i = 0;

            for (BinaryRow row : rows)
                futs[i++] = txManager.writeLock(tableId, row.keySlice(), tx0.timestamp());

            return CompletableFuture.allOf(futs).
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

        return wrapInTx(keyRows, tx, tx0 -> store.getAll(keyRows, tx0.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        CompletableFuture<Void> fut = wrapInTx(rows, tx, tx0 -> {
            store.upsertAll(rows, tx0.timestamp());

            return null;
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return wrapInTx(row, tx, tx0 -> store.insert(row, tx0.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        return wrapInTx(rows, tx, tx0 -> store.insertAll(rows, tx0.timestamp()));
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

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction tx) {
        throw new IgniteInternalException(new OperationNotSupportedException());
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
        return wrapInTx(rows, tx, tx0 -> store.deleteAll(rows, tx0.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
                                                                             InternalTransaction tx) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override public Flow.Publisher<BinaryRow> scan(int p, InternalTransaction tx) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<String> assignments() {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private VersionedRowStore.KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new VersionedRowStore.KeyWrapper(bytes, row.hash());
    }

    /** {@inheritDoc} */
    @Override public int partition(BinaryRow keyRow) {
        return 0;
    }
}
