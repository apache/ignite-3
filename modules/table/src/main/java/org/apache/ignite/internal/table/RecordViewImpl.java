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
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.RecordMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.RecordMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Record view implementation.
 */
public class RecordViewImpl<R> extends AbstractTableView implements RecordView<R> {
    /** Marshaller factory. */
    private final Function<SchemaDescriptor, RecordMarshaller<R>> marshallerFactory;

    /** Record marshaller. */
    private volatile RecordMarshaller<R> marsh;

    /**
     * Constructor.
     *
     * @param tbl       Table.
     * @param schemaReg Schema registry.
     * @param mapper    Record class mapper.
     */
    public RecordViewImpl(InternalTable tbl, SchemaRegistry schemaReg, Mapper<R> mapper) {
        super(tbl, schemaReg);

        marshallerFactory = (schema) -> new RecordMarshallerImpl<>(schema, mapper);
    }

    /** {@inheritDoc} */
    @Override
    public R get(@Nullable Transaction tx, @NotNull R keyRec) {
        return sync(getAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAsync(@Nullable Transaction tx, @NotNull R keyRec) {
        Objects.requireNonNull(keyRec);

        BinaryRow keyRow = marshalKey(keyRec);  // Convert to portable format to pass TX/storage layer.

        return tbl.get(keyRow, (InternalTransaction) tx)  // Load async.
                .thenApply(this::wrap) // Binary -> schema-aware row
                .thenApply(this::unmarshal); // Deserialize.
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> getAll(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, @NotNull R rec) {
        sync(upsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, @NotNull R rec) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(rec));

        return tbl.upsert(keyRow, (InternalTransaction) tx).thenAccept(ignore -> {
        });
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public R getAndUpsert(@Nullable Transaction tx, @NotNull R rec) {
        return sync(getAndUpsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, @NotNull R rec) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(rec));

        return tbl.getAndUpsert(keyRow, (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, @NotNull R rec) {
        return sync(insertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, @NotNull R rec) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(rec));

        return tbl.insert(keyRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> insertAll(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> insertAllAsync(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull R rec) {
        return sync(replaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull R oldRec, @NotNull R newRec) {
        return sync(replaceAsync(tx, oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull R rec) {
        BinaryRow newRow = marshal(rec);

        return tbl.replace(newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull R oldRec, @NotNull R newRec) {
        BinaryRow oldRow = marshal(oldRec);
        BinaryRow newRow = marshal(newRec);

        return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public R getAndReplace(@Nullable Transaction tx, @NotNull R rec) {
        return sync(getAndReplaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, @NotNull R rec) {
        BinaryRow row = marshal(rec);

        return tbl.getAndReplace(row, (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, @NotNull R keyRec) {
        return sync(deleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, @NotNull R keyRec) {
        BinaryRow row = marshalKey(keyRec);

        return tbl.delete(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, @NotNull R rec) {
        return sync(deleteExactAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, @NotNull R rec) {
        BinaryRow row = marshal(rec);

        return tbl.deleteExact(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public R getAndDelete(@Nullable Transaction tx, @NotNull R keyRec) {
        return sync(getAndDeleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, @NotNull R keyRec) {
        BinaryRow row = marshalKey(keyRec);

        return tbl.getAndDelete(row, (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAll(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllAsync(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAllExact(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(@Nullable Transaction tx, @NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
            @Nullable Transaction tx,
            @NotNull R keyRec,
            InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<R, T> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<R> keyRecs,
            InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<R> keyRecs,
            InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
    /**
     * Returns marshaller.
     *
     * @param schemaVersion Schema version.
     * @return Marshaller.
     */
    private RecordMarshaller<R> marshaller(int schemaVersion) {
        RecordMarshaller<R> marsh = this.marsh;

        if (marsh != null && marsh.schemaVersion() == schemaVersion) {
            return marsh;
        }

        // TODO: Cache marshaller for schema version or upgrade row?

        return this.marsh = marshallerFactory.apply(schemaReg.schema(schemaVersion));
    }

    /**
     * Marshals given record to a row.
     *
     * @param rec Record object.
     * @return Binary row.
     */
    private BinaryRow marshal(@NotNull R rec) {
        final RecordMarshaller<R> marsh = marshaller(schemaReg.lastSchemaVersion());

        try {
            return marsh.marshal(rec);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Marshals given key record to a row.
     *
     * @param rec Record key object.
     * @return Binary row.
     */
    private BinaryRow marshalKey(@NotNull R rec) {
        final RecordMarshaller<R> marsh = marshaller(schemaReg.lastSchemaVersion());

        try {
            return marsh.marshalKey(rec);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Unmarshal value object from given binary row.
     *
     * @param binaryRow Binary row.
     * @return Value object.
     */
    private R unmarshal(BinaryRow binaryRow) {
        if (binaryRow == null || !binaryRow.hasValue()) {
            return null;
        }

        Row row = schemaReg.resolve(binaryRow);

        RecordMarshaller<R> marshaller = marshaller(row.schemaVersion());

        try {
            return marshaller.unmarshal(row);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Returns schema-aware row.
     *
     * @param row Binary row.
     */
    private Row wrap(BinaryRow row) {
        if (row == null) {
            return null;
        }

        final SchemaDescriptor rowSchema = schemaReg.schema(row.schemaVersion()); // Get a schema for row.

        return new Row(rowSchema, row);
    }
}
