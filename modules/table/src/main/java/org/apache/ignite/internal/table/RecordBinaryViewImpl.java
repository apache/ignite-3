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

package org.apache.ignite.internal.table;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table view implementation for binary objects.
 */
public class RecordBinaryViewImpl extends AbstractTableView implements RecordView<Tuple> {
    /** Marshaller. */
    private final TupleMarshallerImpl marsh;

    /**
     * Constructor.
     *
     * @param tbl       The table.
     * @param schemaReg Table schema registry.
     */
    public RecordBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg) {
        super(tbl, schemaReg);

        marsh = new TupleMarshallerImpl(schemaReg);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public Tuple get(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return sync(getAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true); // Convert to portable format to pass TX/storage layer.

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(this::wrap);
    }

    @WithSpan
    @Override
    public List<Tuple> getAll(@Nullable Transaction tx, Collection<Tuple> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    @WithSpan
    @Override
    public CompletableFuture<List<Tuple>> getAllAsync(@Nullable Transaction tx, Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return tbl.getAll(mapToBinary(keyRecs, true), (InternalTransaction) tx).thenApply(binaryRows -> wrap(binaryRows, true));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public void upsert(@Nullable Transaction tx, @NotNull Tuple rec) {
        sync(upsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.upsert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public void upsertAll(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.upsertAll(mapToBinary(recs, false), (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public Tuple getAndUpsert(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(getAndUpsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndUpsert(row, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public boolean insert(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(insertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.insert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public Collection<Tuple> insertAll(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.insertAll(mapToBinary(recs, false), (InternalTransaction) tx).thenApply(rows -> wrap(rows, false));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(replaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return sync(replaceAsync(tx, oldRec, newRec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.replace(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull Tuple oldRec, @NotNull Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        final Row oldRow = marshal(oldRec, false);
        final Row newRow = marshal(newRec, false);

        return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public Tuple getAndReplace(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(getAndReplaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndReplace(row, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public boolean delete(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return sync(deleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true);

        return tbl.delete(keyRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public boolean deleteExact(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(deleteExactAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.deleteExact(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public Tuple getAndDelete(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return sync(getAndDeleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true);

        return tbl.getAndDelete(keyRow, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public Collection<Tuple> deleteAll(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return tbl.deleteAll(mapToBinary(keyRecs, true), (InternalTransaction) tx).thenApply(this::wrapKeys);
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public Collection<Tuple> deleteAllExact(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.deleteAllExact(mapToBinary(recs, false), (InternalTransaction) tx).thenApply(rows -> wrap(rows, false));
    }

    /**
     * Marshal a tuple to a row.
     *
     * @param tuple   The tuple.
     * @param keyOnly Marshal key part only if {@code true}, otherwise marshal both, key and value parts.
     * @return Row.
     * @throws IgniteException If failed to marshal tuple.
     */
    @WithSpan
    private Row marshal(@NotNull Tuple tuple, boolean keyOnly) throws IgniteException {
        try {
            if (keyOnly) {
                return marsh.marshalKey(tuple);
            } else {
                return marsh.marshal(tuple);
            }
        } catch (TupleMarshallerException ex) {
            throw convertException(ex);
        }
    }

    /**
     * Returns table row tuple.
     *
     * @param row Binary row.
     */
    @WithSpan
    private @Nullable Tuple wrap(@Nullable BinaryRow row) {
        return row == null ? null : TableRow.tuple(schemaReg.resolve(row));
    }

    /**
     * Returns table rows.
     *
     * @param rows Binary rows.
     * @param addNull {@code true} if {@code null} is added for missing rows.
     */
    @WithSpan
    private List<Tuple> wrap(Collection<BinaryRow> rows, boolean addNull) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        var wrapped = new ArrayList<Tuple>(rows.size());

        for (Row row : schemaReg.resolve(rows)) {
            if (row != null) {
                wrapped.add(TableRow.tuple(row));
            } else if (addNull) {
                wrapped.add(null);
            }
        }

        return wrapped;
    }

    private List<Tuple> wrapKeys(Collection<BinaryRow> rows) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        var wrapped = new ArrayList<Tuple>(rows.size());

        for (Row row : schemaReg.resolveKeys(rows)) {
            if (row != null) {
                wrapped.add(TableRow.tuple(row));
            }
        }

        return wrapped;
    }

    /**
     * Maps a collection of tuples to binary rows.
     *
     * @param rows Tuples.
     * @param key  {@code true} to marshal only a key.
     * @return List of binary rows.
     */
    private Collection<BinaryRowEx> mapToBinary(Collection<Tuple> rows, boolean key) {
        Collection<BinaryRowEx> mapped = new ArrayList<>(rows.size());

        for (Tuple row : rows) {
            mapped.add(marshal(row, key));
        }

        return mapped;
    }

    /** {@inheritDoc} */
    @WithSpan
    @Override
    public CompletableFuture<Void> streamData(Publisher<Tuple> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var partitioner = new TupleStreamerPartitionAwarenessProvider(schemaReg, tbl.partitions());
        StreamerBatchSender<Tuple, Integer> batchSender = (partitionId, items) -> tbl.upsertAll(mapToBinary(items, false), partitionId);

        return DataStreamer.streamData(publisher, options, batchSender, partitioner);
    }
}
