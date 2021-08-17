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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.schema.SchemaMode;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Dummy table storage implementation.
 * TODO asch rename to local table
 */
public class DummyInternalTableImpl implements InternalTable {
    /** In-memory dummy store. */
    private final VersionedRowStore store;

    public DummyInternalTableImpl(VersionedRowStore store) {
        this.store = store;
    }

    /** {@inheritDoc} */
    @Override public @NotNull UUID tableId() {
        return UUID.randomUUID();
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

        return completedFuture(store.get(row, tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        store.upsert(row, tx);

        return completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(@NotNull BinaryRow row,
        InternalTransaction tx) {
        assert row != null;

        return completedFuture(store.getAndUpsert(row, tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return completedFuture(store.delete(row, tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows,
        InternalTransaction tx) {
        assert keyRows != null && !keyRows.isEmpty();

        return completedFuture(store.getAll(keyRows, tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        store.upsertAll(rows, tx);

        return completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return completedFuture(store.insert(row, tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        return completedFuture(store.insertAll(rows, tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return completedFuture(store.replace(row, tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, InternalTransaction tx) {
        assert oldRow != null;
        assert newRow != null;

        return completedFuture(store.replace(oldRow, newRow, tx));
    }

    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow row, InternalTransaction tx) {
        assert row != null;
        assert row.hasValue();

        return completedFuture(store.deleteExact(row, tx));
    }

    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, InternalTransaction tx) {
        return completedFuture(store.getAndDelete(row, tx));
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows,
        InternalTransaction tx) {
        return completedFuture(store.deleteAll(rows, tx));
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        InternalTransaction tx) {
        return completedFuture(store.deleteAllExact(rows, tx));
    }

}
