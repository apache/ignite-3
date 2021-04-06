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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.RecordSerializer;
import org.apache.ignite.lang.IgniteRuntimeException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.RecordMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Record view implementation.
 */
public class RecordViewImpl<R> implements RecordView<R> {
    /** Table */
    private final InternalTable tbl;

    /** Schema manager. */
    private final TableSchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param schemaMgr Schema manager.
     * @param mapper Record class mapper.
     */
    public RecordViewImpl(InternalTable tbl, TableSchemaManager schemaMgr, RecordMapper<R> mapper) {
        this.tbl = tbl;
        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public R fill(R recObjToFill) {
        return sync(fillAsync(recObjToFill));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<R> fillAsync(R recObjToFill) {
        Objects.requireNonNull(recObjToFill);

        RecordSerializer<R> marsh = serializer();

        Row kRow = marsh.serialize(recObjToFill);  // Convert to portable format to pass TX/storage layer.

        return tbl.get(kRow)  // Load async.
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(r -> marsh.deserialize(r, recObjToFill)); // Deserialize and fill record.
    }

    /** {@inheritDoc} */
    @Override public R get(R keyRec) {
        return sync(getAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAsync(R keyRec) {
        Objects.requireNonNull(keyRec);

        RecordSerializer<R> marsh = serializer();

        Row kRow = marsh.serialize(keyRec);  // Convert to portable format to pass TX/storage layer.

        return tbl.get(kRow)  // Load async.
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(marsh::deserialize); // Deserialize.
    }

    /** {@inheritDoc} */
    @Override public Collection<R> getAll(Collection<R> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> getAllAsync(Collection<R> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsert(R rec) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<R> recs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public R getAndUpsert(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAndUpsertAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> insertAll(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> insertAllAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(R oldRec, R newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(R oldRec, R newRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public R getAndReplace(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAndReplaceAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(R keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(R keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public R getAndDelete(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAndDeleteAsync(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> deleteAll(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> deleteAllAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> deleteAllExact(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(R keyRec, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(R keyRec,
        InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<R, T> invokeAll(
        Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(
        Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc
    ) {
        return null;
    }

    /**
     * @return Marshaller.
     */
    private RecordSerializer<R> serializer() {
        return null;
    }

    /**
     * @param row Binary row.
     * @return Schema-aware row.
     */
    private Row wrap(BinaryRow row) {
        if (row == null)
            return null;

        final SchemaDescriptor rowSchema = schemaMgr.schema(row.schemaVersion()); // Get a schema for row.

        return new Row(rowSchema, row);
    }

    /**
     * Waits for operation completion.
     *
     * @param fut Future to wait to.
     * @return Future result.
     */
    private <T> T sync(CompletableFuture<T> fut) {
        try {
            return fut.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag.

            throw new IgniteRuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new IgniteRuntimeException(e);
        }
    }
}
