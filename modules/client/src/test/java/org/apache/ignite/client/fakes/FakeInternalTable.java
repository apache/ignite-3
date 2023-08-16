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

package org.apache.ignite.client.fakes;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiConsumer;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Fake internal table.
 */
public class FakeInternalTable implements InternalTable {
    /** Table name. */
    private final String tableName;

    /** Table ID. */
    private final int tableId;

    private final ColumnsExtractor keyExtractor;

    /** Table data. */
    private final ConcurrentHashMap<ByteBuffer, BinaryRow> data = new ConcurrentHashMap<>();

    /** Data access listener. */
    private BiConsumer<String, Object> dataAccessListener;

    /**
     * The constructor.
     *
     * @param tableName Name.
     * @param tableId Id.
     * @param keyExtractor Function which converts given binary row to an index key.
     */
    public FakeInternalTable(String tableName, int tableId, ColumnsExtractor keyExtractor) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.keyExtractor = keyExtractor;
    }

    /** {@inheritDoc} */
    @Override
    public MvTableStorage storage() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public int partitions() {
        return 1;
    }

    @Override
    public int tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId(BinaryRowEx row) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumnsFromKeyOnlyRow(keyRow);

        return completedFuture(getImpl(key, keyRow));
    }

    @Override
    public CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode) {
        return null;
    }

    private BinaryRow getImpl(BinaryTuple key, BinaryRow keyRow) {
        onDataAccess("get", keyRow);

        return data.get(key.byteBuffer());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, @Nullable InternalTransaction tx) {
        var res = new ArrayList<BinaryRow>();

        for (var key : keyRows) {
            var val = get(key, null);

            if (val != null) {
                res.add(val.getNow(null));
            } else {
                res.add(null);
            }
        }

        onDataAccess("getAll", keyRows);
        return completedFuture(res);
    }

    @Override
    public CompletableFuture<List<BinaryRow>> getAll(
            Collection<BinaryRowEx> keyRows,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        upsertImpl(keyExtractor.extractColumns(row), row);

        return completedFuture(null);
    }

    private void upsertImpl(BinaryTuple key, BinaryRow row) {
        onDataAccess("upsert", row);

        data.put(key.byteBuffer(), row);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        for (var row : rows) {
            upsert(row, tx);
        }

        onDataAccess("upsertAll", rows);
        return completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, int partition) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row,
            @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        BinaryRow res = getImpl(key, row);

        upsertImpl(key, row);

        onDataAccess("getAndUpsert", row);

        return completedFuture(res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        BinaryRow old = getImpl(key, row);

        if (old == null) {
            upsertImpl(key, row);
        }

        onDataAccess("insert", row);

        return completedFuture(old == null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!insert(row, tx).getNow(null)) {
                skipped.add(row);
            }
        }

        onDataAccess("insertAll", rows);
        return completedFuture(skipped);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        return completedFuture(replaceImpl(key, row, tx) != null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(oldRow);

        BinaryRow old = getImpl(key, oldRow);

        if (old == null || !old.tupleSlice().equals(oldRow.tupleSlice())) {
            onDataAccess("replace", oldRow);
            return completedFuture(false);
        }

        upsertImpl(key, newRow);

        onDataAccess("replace", oldRow);
        return completedFuture(true);
    }

    private @Nullable BinaryRow replaceImpl(BinaryTuple key, BinaryRow row, @Nullable InternalTransaction tx) {
        BinaryRow old = getImpl(key, row);

        if (old == null) {
            onDataAccess("replace", row);

            return null;
        }

        upsertImpl(key, row);

        onDataAccess("replace", row);

        return old;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        BinaryRow replace = replaceImpl(key, row, tx);

        onDataAccess("getAndReplace", row);

        return completedFuture(replace);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> delete(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumnsFromKeyOnlyRow(keyRow);

        BinaryRow old = getImpl(key, keyRow);

        if (old != null) {
            data.remove(key.byteBuffer());
        }

        onDataAccess("delete", keyRow);
        return completedFuture(old != null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, @Nullable InternalTransaction tx) {
        var res = false;

        BinaryTuple key = keyExtractor.extractColumns(oldRow);

        BinaryRow old = getImpl(key, oldRow);

        if (old != null && old.tupleSlice().equals(oldRow.tupleSlice())) {
            data.remove(key.byteBuffer());
            res = true;
        }

        onDataAccess("deleteExact", oldRow);
        return completedFuture(res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumnsFromKeyOnlyRow(row);

        BinaryRow old = getImpl(key, row);

        if (old != null) {
            data.remove(key.byteBuffer());
        }

        onDataAccess("getAndDelete", row);
        return completedFuture(old);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!delete(row, tx).getNow(false)) {
                skipped.add(row);
            }
        }

        onDataAccess("deleteAll", rows);
        return completedFuture(skipped);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!deleteExact(row, tx).getNow(false)) {
                skipped.add(row);
            }
        }

        onDataAccess("deleteAllExact", rows);
        return completedFuture(skipped);
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            BitSet columnsToInclude
    ) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            PrimaryReplica recipient,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode
    ) {
        return null;
    }

    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            PrimaryReplica recipient,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude
    ) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude
    ) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public List<String> assignments() {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<PrimaryReplica>> primaryReplicas() {
        return CompletableFuture.failedFuture(new IgniteInternalException(new OperationNotSupportedException()));
    }

    /** {@inheritDoc} */
    @Override
    public ClusterNode leaderAssignment(int partition) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public RaftGroupService partitionRaftGroupService(int partition) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public TxStateTableStorage txStateStorage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int partition(BinaryRowEx keyRow) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        // No-op.
    }

    /**
     * Sets the data access operation listener.
     *
     * @param dataAccessListener Data access operation listener.
     */
    public void setDataAccessListener(BiConsumer<String, Object> dataAccessListener) {
        this.dataAccessListener = dataAccessListener;
    }

    private void onDataAccess(String operation, Object arg) {
        if (dataAccessListener != null) {
            dataAccessListener.accept(operation, arg);
        }
    }

    @Override
    public @Nullable PendingComparableValuesTracker<HybridTimestamp, Void> getPartitionSafeTimeTracker(int partitionId) {
        return null;
    }

    @Override
    public @Nullable PendingComparableValuesTracker<Long, Void> getPartitionStorageIndexTracker(int partitionId) {
        return null;
    }
}
