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
import static org.apache.ignite.internal.util.CompletableFutures.booleanCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.client.handler.FakePlacementDriver;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.compute.streamer.StreamerReceiverJob;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.ReceiverDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Fake internal table.
 */
public class FakeInternalTable implements InternalTable, StreamerReceiverRunner {
    public static final int PARTITIONS = 4;

    /** Table name. */
    private final String tableName;

    /** Table ID. */
    private final int tableId;

    private final ColumnsExtractor keyExtractor;

    /** Table data. */
    private final ConcurrentHashMap<ByteBuffer, BinaryRow> data = new ConcurrentHashMap<>();

    private final IgniteCompute compute;

    private final FakePlacementDriver placementDriver;

    /** Data access listener. */
    private BiConsumer<String, Object> dataAccessListener;

    /**
     * The constructor.
     *
     * @param tableName Name.
     * @param tableId Id.
     * @param keyExtractor Function which converts given binary row to an index key.
     * @param placementDriver Placement driver.
     */
    FakeInternalTable(
            String tableName,
            int tableId,
            ColumnsExtractor keyExtractor,
            IgniteCompute compute,
            FakePlacementDriver placementDriver) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.keyExtractor = keyExtractor;
        this.compute = compute;
        this.placementDriver = placementDriver;
    }

    @Override
    public MvTableStorage storage() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int partitions() {
        return PARTITIONS;
    }

    @Override
    public int tableId() {
        return tableId;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public void name(String newName) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public int partitionId(BinaryRowEx row) {
        return 0;
    }

    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        return completedFuture(getImpl(keyRow.tupleSlice(), keyRow));
    }

    @Override
    public CompletableFuture<BinaryRow> get(
            BinaryRowEx keyRow,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode) {
        return null;
    }

    private BinaryRow getImpl(ByteBuffer key, BinaryRow keyRow) {
        onDataAccess("get", keyRow);

        return data.get(key);
    }

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

    @Override
    public CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        upsertImpl(keyExtractor.extractColumns(row), row);

        return nullCompletedFuture();
    }

    private void upsertImpl(BinaryTuple key, BinaryRow row) {
        onDataAccess("upsert", row);

        data.put(key.byteBuffer(), row);
    }

    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        for (var row : rows) {
            upsert(row, tx);
        }

        onDataAccess("upsertAll", rows);
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> updateAll(Collection<BinaryRowEx> rows, @Nullable BitSet deleted, int partition) {
        int i = 0;

        for (var row : rows) {
            if (deleted != null && deleted.get(i)) {
                delete(row, null);
            } else {
                upsert(row, null);
            }

            i++;
        }

        onDataAccess("updateAll", rows);
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row,
            @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        BinaryRow res = getImpl(key.byteBuffer(), row);

        upsertImpl(key, row);

        onDataAccess("getAndUpsert", row);

        return completedFuture(res);
    }

    @Override
    public CompletableFuture<Boolean> insert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        BinaryRow old = getImpl(key.byteBuffer(), row);

        if (old == null) {
            upsertImpl(key, row);
        }

        onDataAccess("insert", row);

        return booleanCompletedFuture(old == null);
    }

    @Override
    public CompletableFuture<List<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!insert(row, tx).getNow(null)) {
                skipped.add(row);
            }
        }

        onDataAccess("insertAll", rows);
        return completedFuture(skipped);
    }

    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        return booleanCompletedFuture(replaceImpl(key, row, tx) != null);
    }

    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(oldRow);

        BinaryRow old = getImpl(key.byteBuffer(), oldRow);

        if (old == null || !old.tupleSlice().equals(oldRow.tupleSlice())) {
            onDataAccess("replace", oldRow);
            return falseCompletedFuture();
        }

        upsertImpl(key, newRow);

        onDataAccess("replace", oldRow);
        return trueCompletedFuture();
    }

    private @Nullable BinaryRow replaceImpl(BinaryTuple key, BinaryRow row, @Nullable InternalTransaction tx) {
        BinaryRow old = getImpl(key.byteBuffer(), row);

        if (old == null) {
            onDataAccess("replace", row);

            return null;
        }

        upsertImpl(key, row);

        onDataAccess("replace", row);

        return old;
    }

    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryTuple key = keyExtractor.extractColumns(row);

        BinaryRow replace = replaceImpl(key, row, tx);

        onDataAccess("getAndReplace", row);

        return completedFuture(replace);
    }

    @Override
    public CompletableFuture<Boolean> delete(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        BinaryRow old = getImpl(keyRow.tupleSlice(), keyRow);

        if (old != null) {
            data.remove(keyRow.tupleSlice());
        }

        onDataAccess("delete", keyRow);
        return booleanCompletedFuture(old != null);
    }

    @Override
    public CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, @Nullable InternalTransaction tx) {
        var res = false;

        BinaryTuple key = keyExtractor.extractColumns(oldRow);

        BinaryRow old = getImpl(key.byteBuffer(), oldRow);

        if (old != null && old.tupleSlice().equals(oldRow.tupleSlice())) {
            data.remove(key.byteBuffer());
            res = true;
        }

        onDataAccess("deleteExact", oldRow);
        return booleanCompletedFuture(res);
    }

    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, @Nullable InternalTransaction tx) {
        BinaryRow old = getImpl(row.tupleSlice(), row);

        if (old != null) {
            data.remove(row.tupleSlice());
        }

        onDataAccess("getAndDelete", row);
        return completedFuture(old);
    }

    @Override
    public CompletableFuture<List<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!delete(row, tx).getNow(false)) {
                skipped.add(row);
            }
        }

        onDataAccess("deleteAll", rows);
        return completedFuture(skipped);
    }

    @Override
    public CompletableFuture<List<BinaryRow>> deleteAllExact(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
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
            TablePartitionId commitPartition,
            String txCoordinatorId,
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
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            @Nullable Integer indexId,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            @Nullable BitSet columnsToInclude,
            String txCoordinatorId) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            String txCoordinatorId
    ) {
        return null;
    }

    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            UUID txId,
            TablePartitionId commitPartition,
            String txCoordinatorId,
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
            UUID txId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            int indexId,
            BinaryTuple key,
            @Nullable BitSet columnsToInclude,
            String txCoordinatorId
    ) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    @Override public TxStateTableStorage txStateStorage() {
        return null;
    }

    @Override
    public int partition(BinaryRowEx keyRow) {
        return 0;
    }

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

    @Override
    public ScheduledExecutorService streamerFlushExecutor() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<ClusterNode> partitionLocation(TablePartitionId partitionId) {
        List<ReplicaMeta> replicaMetas = placementDriver.primaryReplicas();
        ReplicaMeta replica = replicaMetas.get(partitionId.partitionId());

        //noinspection DataFlowIssue
        return completedFuture(
                new ClusterNodeImpl(
                        replica.getLeaseholderId(),
                        replica.getLeaseholder(),
                        new NetworkAddress("localhost", 10800)));
    }

    @Override
    public CompletableFuture<Long> estimatedSize() {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    @Override
    public StreamerReceiverRunner streamerReceiverRunner() {
        return this;
    }

    @Override
    public <A, I, R> CompletableFuture<Collection<R>> runReceiverAsync(ReceiverDescriptor<A> receiver, @Nullable A receiverArg,
            Collection<I> items, ClusterNode node, List<DeploymentUnit> deploymentUnits) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<byte[]> runReceiverAsync(byte[] payload, ClusterNode node, List<DeploymentUnit> deploymentUnits) {
        return compute.executeAsync(
                JobTarget.node(node),
                JobDescriptor.builder(StreamerReceiverJob.class).units(deploymentUnits).build(),
                payload);
    }
}
