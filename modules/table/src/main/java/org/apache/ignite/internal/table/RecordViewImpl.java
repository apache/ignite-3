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

import static org.apache.ignite.lang.IgniteExceptionMapperUtil.convertToPublicFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.RecordMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.RecordMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
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
    public R get(@Nullable Transaction tx, R keyRec) {
        return sync(getAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAsync(@Nullable Transaction tx, R keyRec) {
        BinaryRowEx keyRow = marshalKey(Objects.requireNonNull(keyRec));

        return convertToPublicFuture(tbl.get(keyRow, (InternalTransaction) tx).thenApply(this::unmarshal));
    }

    @Override
    public List<R> getAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    @Override
    public CompletableFuture<List<R>> getAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return convertToPublicFuture(tbl.getAll(marshalKeys(keyRecs), (InternalTransaction) tx)
                .thenApply(binaryRows -> unmarshal(binaryRows, true)));
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, R rec) {
        sync(upsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, R rec) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(rec));

        return convertToPublicFuture(tbl.upsert(keyRow, (InternalTransaction) tx));
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<R> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        Objects.requireNonNull(recs);

        return convertToPublicFuture(tbl.upsertAll(marshal(recs), (InternalTransaction) tx));
    }

    /** {@inheritDoc} */
    @Override
    public R getAndUpsert(@Nullable Transaction tx, R rec) {
        return sync(getAndUpsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, R rec) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(rec));

        return convertToPublicFuture(tbl.getAndUpsert(keyRow, (InternalTransaction) tx).thenApply(this::unmarshal));
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, R rec) {
        return sync(insertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, R rec) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(rec));

        return convertToPublicFuture(tbl.insert(keyRow, (InternalTransaction) tx));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> insertAll(@Nullable Transaction tx, Collection<R> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<R>> insertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        Collection<BinaryRowEx> rows = marshal(Objects.requireNonNull(recs));

        return convertToPublicFuture(tbl.insertAll(rows, (InternalTransaction) tx).thenApply(binaryRows -> unmarshal(binaryRows, false)));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, R rec) {
        return sync(replaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, R oldRec, R newRec) {
        return sync(replaceAsync(tx, oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R rec) {
        BinaryRowEx newRow = marshal(Objects.requireNonNull(rec));

        return convertToPublicFuture(tbl.replace(newRow, (InternalTransaction) tx));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R oldRec, R newRec) {
        BinaryRowEx oldRow = marshal(Objects.requireNonNull(oldRec));
        BinaryRowEx newRow = marshal(Objects.requireNonNull(newRec));

        return convertToPublicFuture(tbl.replace(oldRow, newRow, (InternalTransaction) tx));
    }

    /** {@inheritDoc} */
    @Override
    public R getAndReplace(@Nullable Transaction tx, R rec) {
        return sync(getAndReplaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, R rec) {
        BinaryRowEx row = marshal(Objects.requireNonNull(rec));

        return convertToPublicFuture(tbl.getAndReplace(row, (InternalTransaction) tx).thenApply(this::unmarshal));
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, R keyRec) {
        return sync(deleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, R keyRec) {
        BinaryRowEx row = marshalKey(Objects.requireNonNull(keyRec));

        return convertToPublicFuture(tbl.delete(row, (InternalTransaction) tx));
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, R rec) {
        return sync(deleteExactAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, R keyRec) {
        BinaryRowEx row = marshal(Objects.requireNonNull(keyRec));

        return convertToPublicFuture(tbl.deleteExact(row, (InternalTransaction) tx));
    }

    /** {@inheritDoc} */
    @Override
    public R getAndDelete(@Nullable Transaction tx, R keyRec) {
        return sync(getAndDeleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, R keyRec) {
        BinaryRowEx row = marshalKey(keyRec);

        return convertToPublicFuture(tbl.getAndDelete(row, (InternalTransaction) tx).thenApply(this::unmarshal));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<R>> deleteAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        Collection<BinaryRowEx> rows = marshal(Objects.requireNonNull(keyRecs));

        return convertToPublicFuture(tbl.deleteAll(rows, (InternalTransaction) tx).thenApply(binaryRows -> unmarshal(binaryRows, false)));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAllExact(@Nullable Transaction tx, Collection<R> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<R>> deleteAllExactAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        Collection<BinaryRowEx> rows = marshal(Objects.requireNonNull(keyRecs));

        return convertToPublicFuture(tbl.deleteAllExact(rows, (InternalTransaction) tx)
                .thenApply(binaryRows -> unmarshal(binaryRows, false)));
    }

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

        SchemaDescriptor schema = rowConverter.registry().schema(schemaVersion);
        return this.marsh = marshallerFactory.apply(schema);
    }

    /**
     * Returns marshaller for the latest schema.
     *
     * @return Marshaller.
     */
    private RecordMarshaller<R> marshaller() {
        SchemaRegistry schemaReg = rowConverter.registry();
        return marshaller(schemaReg.lastSchemaVersion());
    }

    /**
     * Marshals given record to a row.
     *
     * @param rec Record object.
     * @return Binary row.
     */
    private BinaryRowEx marshal(R rec) {
        RecordMarshaller<R> marsh = marshaller();

        try {
            return marsh.marshal(rec);
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Marshal records.
     *
     * @param recs Records collection.
     * @return Binary rows collection.
     */
    private Collection<BinaryRowEx> marshal(Collection<R> recs) {
        RecordMarshaller<R> marsh = marshaller();

        List<BinaryRowEx> rows = new ArrayList<>(recs.size());

        try {
            for (R rec : recs) {
                Row row = marsh.marshal(Objects.requireNonNull(rec));

                rows.add(row);
            }

            return rows;
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Marshals given key record to a row.
     *
     * @param rec Record key object.
     * @return Binary row.
     */
    private BinaryRowEx marshalKey(R rec) {
        RecordMarshaller<R> marsh = marshaller();

        try {
            return marsh.marshalKey(rec);
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Marshal key-records.
     *
     * @param recs Records collection.
     * @return Binary rows collection.
     */
    private Collection<BinaryRowEx> marshalKeys(Collection<R> recs) {
        RecordMarshaller<R> marsh = marshaller();

        List<BinaryRowEx> rows = new ArrayList<>(recs.size());

        try {
            for (R rec : recs) {
                Row row = marsh.marshalKey(Objects.requireNonNull(rec));

                rows.add(row);
            }

            return rows;
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Unmarshal value object from given binary row.
     *
     * @param binaryRow Binary row.
     * @return Value object.
     */
    private @Nullable R unmarshal(@Nullable BinaryRow binaryRow) {
        if (binaryRow == null) {
            return null;
        }

        Row row = rowConverter.resolveRow(binaryRow);

        RecordMarshaller<R> marshaller = marshaller(row.schemaVersion());

        try {
            return marshaller.unmarshal(row);
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Unmarshal records.
     *
     * @param rows Row collection.
     * @param addNull {@code true} if {@code null} is added for missing rows.
     * @return Records collection.
     */
    private List<R> unmarshal(Collection<BinaryRow> rows, boolean addNull) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        RecordMarshaller<R> marsh = marshaller();

        var recs = new ArrayList<R>(rows.size());

        try {
            for (Row row : rowConverter.resolveRows(rows)) {
                if (row != null) {
                    recs.add(marsh.unmarshal(row));
                } else if (addNull) {
                    recs.add(null);
                }
            }

            return recs;
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<R> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var partitioner = new PojoStreamerPartitionAwarenessProvider<>(rowConverter.registry(), tbl.partitions(), marshaller());
        StreamerBatchSender<R, Integer> batchSender = (partitionId, items) -> this.tbl.upsertAll(marshal(items), partitionId);

        return DataStreamer.streamData(publisher, options, batchSender, partitioner);
    }
}
