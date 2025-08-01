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

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.convertToPublicFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ViewUtils.checkCollectionForNulls;
import static org.apache.ignite.internal.util.ViewUtils.checkKeysForNulls;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerSchema;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.marshaller.TupleReader;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.RecordMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.RecordMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.table.criteria.SqlRowProjection;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Record view implementation.
 */
public class RecordViewImpl<R> extends AbstractTableView<R> implements RecordView<R> {
    /** Record class mapper. */
    private final Mapper<R> mapper;

    /** Marshaller factory. */
    private final Function<SchemaDescriptor, RecordMarshaller<R>> marshallerFactory;

    /** Record marshaller. */
    private volatile @Nullable RecordMarshaller<R> marsh;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param schemaRegistry Schema registry.
     * @param schemaVersions Schema versions access.
     * @param sql Ignite SQL facade.
     * @param marshallers Marshallers provider.
     * @param mapper Record class mapper.
     */
    public RecordViewImpl(
            InternalTable tbl,
            SchemaRegistry schemaRegistry,
            SchemaVersions schemaVersions,
            IgniteSql sql,
            MarshallersProvider marshallers,
            Mapper<R> mapper
    ) {
        super(tbl, schemaVersions, schemaRegistry, sql, marshallers);

        this.mapper = mapper;
        marshallerFactory = (schema) -> new RecordMarshallerImpl<>(schema, marshallers, mapper);
    }

    /** {@inheritDoc} */
    @Override
    public R get(@Nullable Transaction tx, R keyRec) {
        return sync(getAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshalKey(keyRec, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx).thenApply(binaryRow -> unmarshal(binaryRow, schemaVersion));
        });
    }

    @Override
    public List<R> getAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    @Override
    public CompletableFuture<List<R>> getAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        checkCollectionForNulls(keyRecs, "keyRecs", "key");

        return doOperation(tx, (schemaVersion) -> {
            return tbl.getAll(marshalKeys(keyRecs, schemaVersion), (InternalTransaction) tx)
                    .thenApply(binaryRows -> unmarshal(binaryRows, false, schemaVersion, true));
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, R keyRec) {
        return sync(containsAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshalKey(keyRec, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx).thenApply(Objects::nonNull);
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<R> keys) {
        return sync(containsAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<R> keys) {
        checkKeysForNulls(keys);

        if (keys.isEmpty()) {
            return trueCompletedFuture();
        }

        return doOperation(tx, (schemaVersion) -> {
            Collection<BinaryRowEx> keyRows = marshalKeys(keys, schemaVersion);

            return tbl.getAll(keyRows, (InternalTransaction) tx).thenApply(rows -> {
                for (BinaryRow row : rows) {
                    if (row == null) {
                        return false;
                    }
                }

                return true;
            });
        });
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, R rec) {
        sync(upsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(rec, schemaVersion);

            return tbl.upsert(keyRow, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, Collection<R> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        checkCollectionForNulls(recs, "recs", "rec");

        return doOperation(tx, (schemaVersion) -> {
            return tbl.upsertAll(marshal(recs, schemaVersion), (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public R getAndUpsert(@Nullable Transaction tx, R rec) {
        return sync(getAndUpsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(rec, schemaVersion);

            return tbl.getAndUpsert(keyRow, (InternalTransaction) tx).thenApply(binaryRow -> unmarshal(binaryRow, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, R rec) {
        return sync(insertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(rec, schemaVersion);

            return tbl.insert(keyRow, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public List<R> insertAll(@Nullable Transaction tx, Collection<R> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<R>> insertAllAsync(@Nullable Transaction tx, Collection<R> recs) {
        checkCollectionForNulls(recs, "recs", "rec");

        return doOperation(tx, (schemaVersion) -> {
            Collection<BinaryRowEx> rows = marshal(recs, schemaVersion);

            return tbl.insertAll(rows, (InternalTransaction) tx)
                    .thenApply(binaryRows -> unmarshal(binaryRows, false, schemaVersion, false));
        });
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
        Objects.requireNonNull(rec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx newRow = marshal(rec, schemaVersion);

            return tbl.replace(newRow, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R oldRec, R newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx oldRow = marshal(oldRec, schemaVersion);
            BinaryRowEx newRow = marshal(newRec, schemaVersion);

            return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public R getAndReplace(@Nullable Transaction tx, R rec) {
        return sync(getAndReplaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, R rec) {
        Objects.requireNonNull(rec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(rec, schemaVersion);

            return tbl.getAndReplace(row, (InternalTransaction) tx).thenApply(binaryRow -> unmarshal(binaryRow, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, R keyRec) {
        return sync(deleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx row = marshalKey(keyRec, schemaVersion);

            return tbl.delete(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, R rec) {
        return sync(deleteExactAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(keyRec, schemaVersion);

            return tbl.deleteExact(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public R getAndDelete(@Nullable Transaction tx, R keyRec) {
        return sync(getAndDeleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, R keyRec) {
        Objects.requireNonNull(keyRec);

        return doOperation(tx, (schemaVersion) -> {
            BinaryRowEx row = marshalKey(keyRec, schemaVersion);

            return tbl.getAndDelete(row, (InternalTransaction) tx).thenApply(binaryRow -> unmarshal(binaryRow, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public List<R> deleteAll(@Nullable Transaction tx, Collection<R> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    @Override
    public void deleteAll(@Nullable Transaction tx) {
        sync(deleteAllAsync(tx));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<R>> deleteAllAsync(@Nullable Transaction tx, Collection<R> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return doOperation(tx, (schemaVersion) -> {
            Collection<BinaryRowEx> rows = marshalKeys(keyRecs, schemaVersion);

            return tbl.deleteAll(rows, (InternalTransaction) tx).thenApply(binaryRows -> unmarshal(binaryRows, true, schemaVersion, false));
        });
    }

    @Override
    public CompletableFuture<Void> deleteAllAsync(@Nullable Transaction tx) {
        return sql.executeAsync(tx, "DELETE FROM " + tbl.name().toCanonicalForm()).thenApply(r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public List<R> deleteAllExact(@Nullable Transaction tx, Collection<R> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<R>> deleteAllExactAsync(@Nullable Transaction tx, Collection<R> recs) {
        Objects.requireNonNull(recs);

        return doOperation(tx, (schemaVersion) -> {
            Collection<BinaryRowEx> rows = marshal(recs, schemaVersion);

            return tbl.deleteAllExact(rows, (InternalTransaction) tx)
                    .thenApply(binaryRows -> unmarshal(binaryRows, true, schemaVersion, false));
        });
    }

    /**
     * Returns marshaller.
     *
     * @param schemaVersion Schema version.
     * @return Marshaller.
     */
    private RecordMarshaller<R> marshaller(int schemaVersion) throws MarshallerException {
        RecordMarshaller<R> marsh = this.marsh;

        if (marsh != null && marsh.schemaVersion() == schemaVersion) {
            return marsh;
        }

        try {
            SchemaDescriptor schema = rowConverter.registry().schema(schemaVersion);

            marsh = marshallerFactory.apply(schema);
            this.marsh = marsh;
        } catch (Exception ex) {
            throw new MarshallerException(ex.getMessage(), ex);
        }

        return marsh;
    }

    /**
     * Marshals given record to a row.
     *
     * @param rec Record object.
     * @param schemaVersion Version with which to marshal.
     * @return Binary row.
     * @throws MarshallerException If failed to marshal row.
     */
    private BinaryRowEx marshal(R rec, int schemaVersion) {
        RecordMarshaller<R> marsh = marshaller(schemaVersion);

        return marsh.marshal(rec);
    }

    /**
     * Marshal records.
     *
     * @param recs Records collection.
     * @param schemaVersion Version with which to marshal.
     * @return Binary rows collection.
     * @throws MarshallerException If failed to marshal rows.
     */
    private Collection<BinaryRowEx> marshal(Collection<R> recs, int schemaVersion) {
        RecordMarshaller<R> marsh = marshaller(schemaVersion);

        List<BinaryRowEx> rows = new ArrayList<>(recs.size());

        for (R rec : recs) {
            BinaryRowEx row = marsh.marshal(Objects.requireNonNull(rec));

            rows.add(row);
        }

        return rows;
    }

    private Collection<BinaryRowEx> marshal(Collection<R> recs, int schemaVersion, @Nullable BitSet deleted) {
        RecordMarshaller<R> marsh = marshaller(schemaVersion);

        List<BinaryRowEx> rows = new ArrayList<>(recs.size());

        for (R rec : recs) {
            boolean isDeleted = deleted != null && deleted.get(rows.size());
            BinaryRowEx row = isDeleted ? marsh.marshalKey(rec) : marsh.marshal(rec);

            rows.add(row);
        }

        return rows;
    }

    /**
     * Marshals given key record to a row.
     *
     * @param rec Record key object.
     * @param schemaVersion Version with which to marshal.
     * @return Binary row.
     */
    private BinaryRowEx marshalKey(R rec, int schemaVersion) {
        RecordMarshaller<R> marsh = marshaller(schemaVersion);

        return marsh.marshalKey(rec);
    }

    /**
     * Marshal key-records.
     *
     * @param recs Records collection.
     * @param schemaVersion Version with which to marshal.
     * @return Binary rows collection.
     */
    private Collection<BinaryRowEx> marshalKeys(Collection<R> recs, int schemaVersion) {
        RecordMarshaller<R> marsh = marshaller(schemaVersion);

        List<BinaryRowEx> rows = new ArrayList<>(recs.size());

        for (R rec : recs) {
            BinaryRowEx row = marsh.marshalKey(Objects.requireNonNull(rec));

            rows.add(row);
        }

        return rows;
    }

    /**
     * Unmarshal value object from given binary row.
     *
     * @param binaryRow Binary row.
     * @param targetSchemaVersion Schema version that should be used.
     * @return Value object.
     */
    private @Nullable R unmarshal(@Nullable BinaryRow binaryRow, int targetSchemaVersion) {
        if (binaryRow == null) {
            return null;
        }

        Row row = rowConverter.resolveRow(binaryRow, targetSchemaVersion);

        RecordMarshaller<R> marshaller = marshaller(row.schemaVersion());

        return marshaller.unmarshal(row);
    }

    /**
     * Unmarshal records.
     *
     * @param rows Row collection.
     * @param addNull {@code true} if {@code null} is added for missing rows.
     * @param keyOnly If rows are key-only.
     * @param targetSchemaVersion Schema version that should be used.
     * @return Records collection.
     */
    private List<R> unmarshal(Collection<BinaryRow> rows, boolean keyOnly, int targetSchemaVersion, boolean addNull) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        RecordMarshaller<R> marsh = marshaller(targetSchemaVersion);

        var recs = new ArrayList<R>(rows.size());
        List<Row> resolvedRows = keyOnly
                ? rowConverter.resolveKeys(rows, targetSchemaVersion)
                : rowConverter.resolveRows(rows, targetSchemaVersion);

        for (Row row : resolvedRows) {
            if (row != null) {
                recs.add(marsh.unmarshal(row));
            } else if (addNull) {
                recs.add(null);
            }
        }

        return recs;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<R>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        @SuppressWarnings({"rawtypes", "unchecked"})
        StreamerBatchSender<R, Integer, Void> batchSender = (partitionId, items, deleted) ->
                PublicApiThreading.execUserAsyncOperation(() -> (CompletableFuture) withSchemaSync(
                        null,
                        schemaVersion -> this.tbl.updateAll(marshal(items, schemaVersion, deleted), deleted, partitionId)
                ));

        CompletableFuture<Void> future = DataStreamer.streamData(
                publisher, options, batchSender, streamerPartitioner(), tbl.streamerFlushExecutor());

        return convertToPublicFuture(future);
    }

    @Override
    public <E, V, A, R1> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            DataStreamerReceiverDescriptor<V, A, R1> receiver,
            Function<E, R> keyFunc,
            Function<E, V> payloadFunc,
            @Nullable A receiverArg,
            @Nullable Flow.Subscriber<R1> resultSubscriber,
            @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);
        Objects.requireNonNull(keyFunc);
        Objects.requireNonNull(payloadFunc);
        Objects.requireNonNull(receiver);

        StreamerBatchSender<V, Integer, R1> batchSender = (partitionIndex, rows, deleted) ->
                PublicApiThreading.execUserAsyncOperation(() ->
                        tbl.partitionLocation(partitionIndex)
                                .thenCompose(node -> tbl.streamerReceiverRunner().runReceiverAsync(
                                        receiver, receiverArg, rows, node, receiver.units())));

        CompletableFuture<Void> future = DataStreamer.streamData(
                publisher,
                keyFunc,
                payloadFunc,
                x -> false,
                options,
                batchSender,
                resultSubscriber,
                streamerPartitioner(),
                tbl.streamerFlushExecutor());

        return convertToPublicFuture(future);
    }

    private PojoStreamerPartitionAwarenessProvider<R> streamerPartitioner() {
        // Taking latest schema version for marshaller here because it's only used to calculate colocation hash, and colocation
        // columns never change (so they are the same for all schema versions of the table),
        return new PojoStreamerPartitionAwarenessProvider<>(
                rowConverter.registry(),
                tbl.partitions(),
                marshaller(rowConverter.registry().lastKnownSchemaVersion())
        );
    }

    /** {@inheritDoc} */
    @Override
    protected Function<SqlRow, R> queryMapper(ResultSetMetadata meta, SchemaDescriptor schema) {
        MarshallerSchema marshallerSchema = schema.marshallerSchema();
        Marshaller marsh = marshallers.getRowMarshaller(marshallerSchema, mapper, false, true);
        List<Column> cols = schema.columns();

        return (row) -> (R) marsh.readObject(new TupleReader(new SqlRowProjection(row, meta, columnNames(cols))), null);
    }
}
