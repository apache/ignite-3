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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import static org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.SnapshotMetaUtils.collectNextRowIdToBuildIndexes;
import static org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.SnapshotMetaUtils.snapshotMetaAt;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replica.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotMetaRequest;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotMvDataRequest;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotTxDataRequest;
import org.apache.ignite.internal.partition.replica.network.raft.SnapshotTxDataResponse;
import org.apache.ignite.internal.partition.replica.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

/**
 * Outgoing snapshot. It corresponds to exactly one partition.
 *
 * <p>The snapshot has a lock over MV data needed for interaction with {@link SnapshotAwarePartitionDataStorage}.
 */
public class OutgoingSnapshot {
    private static final IgniteLogger LOG = Loggers.forClass(OutgoingSnapshot.class);

    private static final PartitionReplicationMessagesFactory MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    private final UUID id;

    private final PartitionAccess partition;

    private final CatalogService catalogService;

    /**
     * Lock that is used for mutual exclusion of MV snapshot reading (by this class) and threads that write MV data to the same
     * partition (currently, via {@link SnapshotAwarePartitionDataStorage}).
     */
    private final ReentrantLock mvOperationsLock = new ReentrantLock();

    /** Snapshot metadata that is taken at the moment when snapshot scope is frozen. {@code null} till the freeze happens. */
    @Nullable
    private volatile SnapshotMeta frozenMeta;

    /**
     * {@link RowId}s for which the corresponding rows were sent out of order (relative to the order in which this
     * snapshot sends rows), hence they must be skipped when sending rows normally.
     */
    private final Set<RowId> rowIdsToSkip = new ConcurrentHashSet<>();

    // TODO: IGNITE-18018 - manage queue size
    /**
     * Rows that need to be sent out of order (relative to the order in which this snapshot sends rows).
     * Versions inside rows are in oldest-to-newest order.
     */
    private final Queue<SnapshotMvDataResponse.ResponseEntry> outOfOrderMvData = new ArrayDeque<>();

    /**
     * {@link RowId} used to point (most of the time) to the last processed row. More precisely:
     *
     * <ul>
     *     <li>Before we started to read from the partition, this is equal to lowest theoretically possible
     *     {@link RowId} for this partition</li>
     *     <li>If we started to read from partition AND it is not yet exhausted, this is the last RowId that was
     *     sent in this snapshot order</li>
     *     <li>After we exhausted the partition, this is {@code null}</li>
     * </ul>
     */
    private RowId lastRowId;

    private boolean startedToReadMvPartition = false;

    private Cursor<IgniteBiTuple<UUID, TxMeta>> txDataCursor;

    /**
     * This becomes {@code true} as soon as we exhaust TX data in the partition.
     */
    private boolean finishedTxData = false;

    private volatile boolean closed = false;

    /**
     * Creates a new instance.
     */
    public OutgoingSnapshot(UUID id, PartitionAccess partition, CatalogService catalogService) {
        this.id = id;
        this.partition = partition;
        this.catalogService = catalogService;

        lastRowId = RowId.lowestRowId(partition.partitionKey().partitionId());
    }

    /**
     * Returns the ID of this snapshot.
     */
    public UUID id() {
        return id;
    }

    /**
     * Returns the key of the corresponding partition.
     *
     * @return Partition key.
     */
    public PartitionKey partitionKey() {
        return partition.partitionKey();
    }

    /**
     * Freezes the scope of this snapshot. This includes taking snapshot metadata and opening TX data cursor.
     */
    void freezeScopeUnderMvLock() {
        acquireMvLock();

        try {
            frozenMeta = takeSnapshotMeta();

            txDataCursor = partition.getAllTxMeta();
        } finally {
            releaseMvLock();
        }
    }

    private SnapshotMeta takeSnapshotMeta() {
        long lastAppliedIndex = partition.maxLastAppliedIndex();
        long lastAppliedTerm = partition.maxLastAppliedTerm();

        RaftGroupConfiguration config = partition.committedGroupConfiguration();

        assert config != null : "Configuration should never be null when installing a snapshot";

        int catalogVersion = catalogService.latestCatalogVersion();

        Map<Integer, UUID> nextRowIdToBuildByIndexId = collectNextRowIdToBuildIndexes(catalogService, partition, catalogVersion);

        return snapshotMetaAt(lastAppliedIndex, lastAppliedTerm, config, catalogVersion, nextRowIdToBuildByIndexId);
    }

    /**
     * Returns metadata corresponding to this snapshot.
     *
     * @return This snapshot metadata.
     */
    public SnapshotMeta meta() {
        SnapshotMeta meta = frozenMeta;

        assert meta != null : "No snapshot meta yet, probably the snapshot scope was not yet frozen";

        return meta;
    }

    /**
     * Reads the snapshot meta and returns a response. Returns {@code null} if the snapshot is already closed.
     *
     * @param request Meta request.
     */
    @Nullable
    SnapshotMetaResponse handleSnapshotMetaRequest(SnapshotMetaRequest request) {
        assert Objects.equals(request.id(), id) : "Expected id " + id + " but got " + request.id();

        if (closed) {
            return logThatAlreadyClosedAndReturnNull();
        }

        SnapshotMeta meta = frozenMeta;

        assert meta != null : "No snapshot meta yet, probably the snapshot scope was not yet frozen";

        return MESSAGES_FACTORY.snapshotMetaResponse().meta(meta).build();
    }

    @Nullable
    private <T> T logThatAlreadyClosedAndReturnNull() {
        LOG.debug("Snapshot with ID '{}' is already closed", id);
        return null;
    }

    /**
     * Reads a chunk of partition data and returns a response. Returns {@code null} if the snapshot is already closed.
     *
     * @param request Data request.
     */
    @Nullable
    SnapshotMvDataResponse handleSnapshotMvDataRequest(SnapshotMvDataRequest request) {
        if (closed) {
            return logThatAlreadyClosedAndReturnNull();
        }

        assert !finishedMvData() : "MV data sending has already been finished";

        long totalBatchSize = 0;
        List<SnapshotMvDataResponse.ResponseEntry> batch = new ArrayList<>();

        while (true) {
            acquireMvLock();

            try {
                totalBatchSize = fillWithOutOfOrderRows(batch, totalBatchSize, request);

                totalBatchSize = tryProcessRowFromPartition(batch, totalBatchSize, request);

                // As out-of-order rows are added under the same lock that we hold, and we always send OOO data first,
                // exhausting the partition means that no MV data to send is left, we are finished with it.
                if (finishedMvData() || batchIsFull(request, totalBatchSize)) {
                    break;
                }
            } finally {
                releaseMvLock();
            }
        }

        return MESSAGES_FACTORY.snapshotMvDataResponse()
                .rows(batch)
                .finish(finishedMvData())
                .build();
    }

    private long fillWithOutOfOrderRows(
            List<SnapshotMvDataResponse.ResponseEntry> rowEntries,
            long totalBytesBefore,
            SnapshotMvDataRequest request
    ) {
        assert mvOperationsLock.isLocked() : "MV operations lock must be acquired!";

        long totalBytesAfter = totalBytesBefore;

        while (totalBytesAfter < request.batchSizeHint()) {
            SnapshotMvDataResponse.ResponseEntry rowEntry = outOfOrderMvData.poll();

            if (rowEntry == null) {
                break;
            }

            rowEntries.add(rowEntry);
            totalBytesAfter += rowSizeInBytes(rowEntry.rowVersions());
        }

        return totalBytesAfter;
    }

    private static long rowSizeInBytes(List<BinaryRowMessage> rowVersions) {
        long sum = 0;

        for (BinaryRowMessage rowMessage : rowVersions) {
            if (rowMessage != null) {
                // Schema version is an unsigned short.
                sum += rowMessage.binaryTuple().remaining() + Short.BYTES;
            }
        }

        return sum;
    }

    private long tryProcessRowFromPartition(List<SnapshotMvDataResponse.ResponseEntry> batch, long totalBatchSize,
            SnapshotMvDataRequest request) {
        if (batchIsFull(request, totalBatchSize) || finishedMvData()) {
            return totalBatchSize;
        }

        if (!startedToReadMvPartition) {
            lastRowId = partition.closestRowId(lastRowId);

            startedToReadMvPartition = true;
        } else {
            lastRowId = partition.closestRowId(lastRowId.increment());
        }

        if (!finishedMvData()) {
            if (!rowIdsToSkip.remove(lastRowId)) {
                SnapshotMvDataResponse.ResponseEntry rowEntry = rowEntry(lastRowId);

                assert rowEntry != null;

                batch.add(rowEntry);

                totalBatchSize += rowSizeInBytes(rowEntry.rowVersions());
            }
        }

        return totalBatchSize;
    }

    private static boolean batchIsFull(SnapshotMvDataRequest request, long totalBatchSize) {
        return totalBatchSize >= request.batchSizeHint();
    }

    @Nullable
    private SnapshotMvDataResponse.ResponseEntry rowEntry(RowId rowId) {
        List<ReadResult> rowVersionsN2O = partition.getAllRowVersions(rowId);

        if (rowVersionsN2O.isEmpty()) {
            return null;
        }

        int count = rowVersionsN2O.size();
        List<BinaryRowMessage> rowVersions = new ArrayList<>(count);

        int commitTimestampsCount = rowVersionsN2O.get(0).isWriteIntent() ? count - 1 : count;
        long[] commitTimestamps = new long[commitTimestampsCount];

        UUID transactionId = null;
        Integer commitTableId = null;
        int commitPartitionId = ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

        for (int i = count - 1, j = 0; i >= 0; i--) {
            ReadResult version = rowVersionsN2O.get(i);
            BinaryRow row = version.binaryRow();

            BinaryRowMessage rowMessage = row == null
                    ? null
                    : MESSAGES_FACTORY.binaryRowMessage()
                            .binaryTuple(row.tupleSlice())
                            .schemaVersion(row.schemaVersion())
                            .build();

            rowVersions.add(rowMessage);

            if (version.isWriteIntent()) {
                assert i == 0 : rowVersionsN2O;

                transactionId = version.transactionId();
                commitTableId = version.commitTableId();
                commitPartitionId = version.commitPartitionId();
            } else {
                commitTimestamps[j++] = version.commitTimestamp().longValue();
            }
        }

        return MESSAGES_FACTORY.responseEntry()
                .rowId(rowId.uuid())
                .rowVersions(rowVersions)
                .timestamps(commitTimestamps)
                .txId(transactionId)
                .commitTableId(commitTableId)
                .commitPartitionId(commitPartitionId)
                .build();
    }

    /**
     * Reads a chunk of TX states from partition and returns a response. Returns {@code null} if the snapshot is already closed.
     *
     * @param request Data request.
     */
    @Nullable
    SnapshotTxDataResponse handleSnapshotTxDataRequest(SnapshotTxDataRequest request) {
        if (closed) {
            return logThatAlreadyClosedAndReturnNull();
        }

        List<IgniteBiTuple<UUID, TxMeta>> rows = new ArrayList<>();

        while (!finishedTxData && rows.size() < request.maxTransactionsInBatch()) {
            if (txDataCursor.hasNext()) {
                rows.add(txDataCursor.next());
            } else {
                finishedTxData = true;
                closeLoggingProblems(txDataCursor);
            }
        }

        return buildTxDataResponse(rows, finishedTxData);
    }

    private static void closeLoggingProblems(Cursor<?> cursor) {
        try {
            cursor.close();
        } catch (RuntimeException e) {
            LOG.error("Problem while closing a cursor", e);
        }
    }

    private static SnapshotTxDataResponse buildTxDataResponse(List<IgniteBiTuple<UUID, TxMeta>> rows, boolean finished) {
        List<UUID> txIds = new ArrayList<>();
        List<TxMeta> txMetas = new ArrayList<>();

        for (IgniteBiTuple<UUID, TxMeta> row : rows) {
            txIds.add(row.getKey());
            txMetas.add(row.getValue());
        }

        return MESSAGES_FACTORY.snapshotTxDataResponse()
                .txIds(txIds)
                .txMeta(txMetas)
                .finish(finished)
                .build();
    }

    /**
     * Acquires lock over this snapshot MV data.
     */
    public void acquireMvLock() {
        mvOperationsLock.lock();
    }

    /**
     * Releases lock over this snapshot MV data.
     */
    public void releaseMvLock() {
        mvOperationsLock.unlock();
    }

    /**
     * Whether this snapshot is finished with sending MV data (i.e. it already sent all the MV data and is not going to send anything else).
     *
     * <p>Must be called under snapshot lock.
     *
     * @return {@code true} if finished.
     */
    private boolean finishedMvData() {
        return lastRowId == null;
    }

    /**
     * Adds a {@link RowId} to the collection of IDs that need to be skipped during normal snapshot MV row sending.
     *
     * <p>Must be called under MV data snapshot lock.
     *
     * @param rowId RowId to add.
     * @return {@code true} if the given RowId was added as it was not yet in the collection of IDs to skip.
     */
    public boolean addRowIdToSkip(RowId rowId) {
        assert mvOperationsLock.isLocked() : "MV operations lock must be acquired!";

        return rowIdsToSkip.add(rowId);
    }

    /**
     * Returns {@code true} if the given {@link RowId} does not interfere with the rows that this snapshot is going
     * to send in the normal snapshot rows sending order.
     *
     * <p>Must be called under MV data snapshot lock.
     *
     * @param rowId RowId.
     * @return {@code true} if the given RowId is already passed by the snapshot in normal rows sending order.
     */
    public boolean alreadyPassed(RowId rowId) {
        assert mvOperationsLock.isLocked() : "MV operations lock must be acquired!";

        if (!startedToReadMvPartition) {
            return false;
        }
        if (finishedMvData()) {
            return true;
        }

        return rowId.compareTo(lastRowId) <= 0;
    }

    /**
     * Enqueues a row for out-of-order sending.
     *
     * <p>Must be called under snapshot lock.
     *
     * @param rowId {@link RowId} of the row.
     */
    public void enqueueForSending(RowId rowId) {
        assert mvOperationsLock.isLocked() : "MV operations lock must be acquired!";

        ResponseEntry entry = rowEntry(rowId);

        if (entry != null) {
            outOfOrderMvData.add(entry);
        }
    }

    /**
     * Closes the snapshot releasing the underlying resources.
     */
    public void close() {
        Cursor<IgniteBiTuple<UUID, TxMeta>> txCursor = txDataCursor;

        if (txCursor != null) {
            closeLoggingProblems(txCursor);
        }

        closed = true;
    }
}
