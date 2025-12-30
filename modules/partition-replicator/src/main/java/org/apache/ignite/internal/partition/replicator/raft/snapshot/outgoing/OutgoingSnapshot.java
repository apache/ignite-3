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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.collectNextRowIdToBuildIndexes;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.snapshotMetaAt;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMeta;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataResponse;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics.RaftSnapshotsMetricsSource;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxMetaMessage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Outgoing snapshot. It corresponds to exactly one partition.
 *
 * <p>The snapshot has a lock over MV data needed for interaction with {@link PartitionDataStorage}.
 */
public class OutgoingSnapshot {
    private static final IgniteLogger LOG = Loggers.forClass(OutgoingSnapshot.class);

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private final UUID id;

    private final PartitionKey partitionKey;

    private final Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId;

    private final PartitionTxStateAccess txState;

    private final CatalogService catalogService;

    /**
     * Lock that is used for mutual exclusion of MV snapshot reading (by this class) and threads that write MV data to the same partition
     * (currently, via {@link PartitionDataStorage}).
     */
    private final ReentrantLock mvOperationsLock = new ReentrantLock();

    /** Snapshot metadata that is taken at the moment when snapshot scope is frozen. {@code null} till the freeze happens. */
    @Nullable
    private volatile PartitionSnapshotMeta frozenMeta;

    /**
     * {@link RowId}s for which the corresponding rows were sent out of order (relative to the order in which this snapshot sends rows),
     * hence they must be skipped when sending rows normally.
     *
     * <p>Multi-threaded access is guarded by {@link #mvOperationsLock}.
     */
    private final Set<RowId> rowIdsToSkip = new HashSet<>();

    // TODO: IGNITE-18018 - manage queue size
    /**
     * Rows that need to be sent out of order (relative to the order in which this snapshot sends rows). Versions inside rows are in
     * oldest-to-newest order.
     *
     * <p>Multi-threaded access is guarded by {@link #mvOperationsLock}.
     */
    private final Queue<SnapshotMvDataResponse.ResponseEntry> outOfOrderMvData = new ArrayDeque<>();

    /**
     * Current delivery state of MV partition data.
     *
     * <p>Multi-threaded access is guarded by {@link #mvOperationsLock}.
     */
    @Nullable
    private MvPartitionDeliveryState mvPartitionDeliveryState;

    /**
     * Cursor over TX data.
     *
     * <p>Inter-thread visibility is provided by accessing {@link #finishedTxData} in a correct order.
     */
    @Nullable
    private Cursor<IgniteBiTuple<UUID, TxMeta>> txDataCursor;

    /**
     * This becomes {@code true} as soon as we exhaust TX data in the partition.
     */
    private volatile boolean finishedTxData;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final AtomicBoolean closedGuard = new AtomicBoolean();

    private final OutgoingSnapshotStats snapshotStats;

    private final RaftSnapshotsMetricsSource snapshotsMetricsSource;

    /**
     * Creates a new instance.
     */
    public OutgoingSnapshot(
            UUID id,
            PartitionKey partitionKey,
            Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId,
            PartitionTxStateAccess txState,
            CatalogService catalogService,
            RaftSnapshotsMetricsSource snapshotMetricsSource
    ) {
        this.id = id;
        this.partitionKey = partitionKey;
        this.partitionsByTableId = partitionsByTableId;
        this.txState = txState;
        this.catalogService = catalogService;
        this.snapshotStats = new OutgoingSnapshotStats(id, partitionKey);
        this.snapshotsMetricsSource = snapshotMetricsSource;
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
        return partitionKey;
    }

    /**
     * Freezes the scope of this snapshot. This includes taking snapshot metadata and opening TX data cursor.
     */
    void freezeScopeUnderMvLock() {
        acquireMvLock();

        try {
            snapshotStats.onSnapshotStart();
            snapshotsMetricsSource.onOutgoingSnapshotStart();

            int catalogVersion = catalogService.latestCatalogVersion();

            List<PartitionMvStorageAccess> partitionStorages = freezePartitionStorages();

            frozenMeta = takeSnapshotMeta(catalogVersion, partitionStorages);

            txDataCursor = txState.getAllTxMeta();

            // Write the flag to publish the TX cursor (in the memory model sense).
            finishedTxData = false;

            mvPartitionDeliveryState = new MvPartitionDeliveryState(partitionStorages);
        } finally {
            releaseMvLock();
        }
    }

    private PartitionSnapshotMeta takeSnapshotMeta(int catalogVersion, Collection<PartitionMvStorageAccess> partitionStorages) {
        Map<Integer, UUID> nextRowIdToBuildByIndexId = collectNextRowIdToBuildIndexes(
                catalogService,
                partitionStorages,
                catalogVersion
        );

        PartitionMvStorageAccess partitionStorageWithMaxAppliedIndex = partitionStorages.stream()
                .max(comparingLong(PartitionMvStorageAccess::lastAppliedIndex))
                .orElse(null);

        if (partitionStorageWithMaxAppliedIndex == null
                || txState.lastAppliedIndex() > partitionStorageWithMaxAppliedIndex.lastAppliedIndex()) {
            RaftGroupConfiguration config = txState.committedGroupConfiguration();

            assert config != null : "Configuration should never be null when installing a snapshot";

            snapshotStats.setSnapshotMeta(txState.lastAppliedIndex(), txState.lastAppliedTerm(), config, catalogVersion);

            return snapshotMetaAt(
                    txState.lastAppliedIndex(),
                    txState.lastAppliedTerm(),
                    config,
                    catalogVersion,
                    nextRowIdToBuildByIndexId,
                    txState.leaseInfo()
            );
        } else {
            RaftGroupConfiguration config = partitionStorageWithMaxAppliedIndex.committedGroupConfiguration();

            assert config != null : "Configuration should never be null when installing a snapshot";

            snapshotStats.setSnapshotMeta(
                    partitionStorageWithMaxAppliedIndex.lastAppliedIndex(),
                    partitionStorageWithMaxAppliedIndex.lastAppliedTerm(),
                    config,
                    catalogVersion
            );

            return snapshotMetaAt(
                    partitionStorageWithMaxAppliedIndex.lastAppliedIndex(),
                    partitionStorageWithMaxAppliedIndex.lastAppliedTerm(),
                    config,
                    catalogVersion,
                    nextRowIdToBuildByIndexId,
                    partitionStorageWithMaxAppliedIndex.leaseInfo()
            );
        }
    }

    private List<PartitionMvStorageAccess> freezePartitionStorages() {
        if (partitionKey instanceof PartitionKey) {
            return partitionsByTableId.values().stream()
                    .sorted(comparingInt(PartitionMvStorageAccess::tableId))
                    .collect(toList());
        } else {
            // TODO: remove this clause, see https://issues.apache.org/jira/browse/IGNITE-22522
            // For a non-colocation case we always have a single entry in this map.
            assert partitionsByTableId.size() == 1;

            return List.copyOf(partitionsByTableId.values());
        }
    }

    /**
     * Returns metadata corresponding to this snapshot.
     *
     * @return This snapshot metadata.
     */
    public PartitionSnapshotMeta meta() {
        PartitionSnapshotMeta meta = frozenMeta;

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

        if (!busyLock.enterBusy()) {
            return logThatAlreadyClosedAndReturnNull();
        }

        try {
            PartitionSnapshotMeta meta = frozenMeta;

            assert meta != null : "No snapshot meta yet, probably the snapshot scope was not yet frozen";

            return PARTITION_REPLICATION_MESSAGES_FACTORY.snapshotMetaResponse().meta(meta).build();
        } finally {
            busyLock.leaveBusy();
        }
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
        if (!busyLock.enterBusy()) {
            return logThatAlreadyClosedAndReturnNull();
        }

        try {
            return handleSnapshotMvDataRequestInternal(request);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private SnapshotMvDataResponse handleSnapshotMvDataRequestInternal(SnapshotMvDataRequest request) {
        long totalBatchSize = 0;
        List<ResponseEntry> batch = new ArrayList<>();

        snapshotStats.onStartMvDataBatchProcessing();

        while (true) {
            acquireMvLock();

            try {
                totalBatchSize = fillWithOutOfOrderRows(batch, totalBatchSize, request);

                totalBatchSize = tryProcessRowFromPartition(batch, totalBatchSize, request);

                // As out-of-order rows are added under the same lock that we hold, and we always send OOO data first,
                // exhausting the partition means that no MV data to send is left, we are finished with it.
                if (finishedMvData() || batchIsFull(request, totalBatchSize)) {
                    snapshotStats.onEndMvDataBatchProcessing();

                    return PARTITION_REPLICATION_MESSAGES_FACTORY.snapshotMvDataResponse()
                            .rows(batch)
                            .finish(finishedMvData())
                            .build();
                }
            } finally {
                releaseMvLock();
            }
        }
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

            long rowSize = rowSizeInBytes(rowEntry.rowVersions());

            snapshotStats.onProcessOutOfOrderRow(rowEntry.rowVersions().size(), rowSize);

            totalBytesAfter += rowSize;
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

    private long tryProcessRowFromPartition(
            List<SnapshotMvDataResponse.ResponseEntry> batch,
            long totalBatchSize,
            SnapshotMvDataRequest request
    ) {
        if (batchIsFull(request, totalBatchSize) || finishedMvData()) {
            return totalBatchSize;
        }

        assert mvPartitionDeliveryState != null : "Snapshot scope has not been frozen.";

        mvPartitionDeliveryState.advance();

        if (!finishedMvData()) {
            RowId rowId = mvPartitionDeliveryState.currentRowId();

            PartitionMvStorageAccess partition = mvPartitionDeliveryState.currentPartitionStorage();

            if (!rowIdsToSkip.remove(rowId)) {
                SnapshotMvDataResponse.ResponseEntry rowEntry = rowEntry(partition, rowId);

                assert rowEntry != null;

                batch.add(rowEntry);

                long rowSize = rowSizeInBytes(rowEntry.rowVersions());

                totalBatchSize += rowSize;

                snapshotStats.onProcessRegularRow(rowEntry.rowVersions().size(), rowSize);
            }
        }

        return totalBatchSize;
    }

    private static boolean batchIsFull(SnapshotMvDataRequest request, long totalBatchSize) {
        return totalBatchSize >= request.batchSizeHint();
    }

    @Nullable
    private static SnapshotMvDataResponse.ResponseEntry rowEntry(PartitionMvStorageAccess partition, RowId rowId) {
        List<ReadResult> rowVersionsN2O = partition.getAllRowVersions(rowId);

        if (rowVersionsN2O.isEmpty()) {
            return null;
        }

        int count = rowVersionsN2O.size();
        List<BinaryRowMessage> rowVersions = new ArrayList<>(count);

        int commitTimestampsCount = rowVersionsN2O.get(0).isWriteIntent() ? count - 1 : count;
        long[] commitTimestamps = new long[commitTimestampsCount];

        UUID transactionId = null;
        Integer commitTableOrZoneId = null;
        int commitPartitionId = ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

        for (int i = count - 1, j = 0; i >= 0; i--) {
            ReadResult version = rowVersionsN2O.get(i);
            BinaryRow row = version.binaryRow();

            BinaryRowMessage rowMessage = row == null
                    ? null
                    : PARTITION_REPLICATION_MESSAGES_FACTORY.binaryRowMessage()
                            .binaryTuple(row.tupleSlice())
                            .schemaVersion(row.schemaVersion())
                            .build();

            rowVersions.add(rowMessage);

            if (version.isWriteIntent()) {
                assert i == 0 : rowVersionsN2O;

                transactionId = version.transactionId();
                commitTableOrZoneId = version.commitZoneId();
                commitPartitionId = version.commitPartitionId();
            } else {
                commitTimestamps[j++] = version.commitTimestamp().longValue();
            }
        }

        return PARTITION_REPLICATION_MESSAGES_FACTORY.responseEntry()
                .tableId(partition.tableId())
                .rowId(rowId.uuid())
                .rowVersions(rowVersions)
                .timestamps(commitTimestamps)
                .txId(transactionId)
                .commitTableOrZoneId(commitTableOrZoneId)
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
        if (!busyLock.enterBusy()) {
            return logThatAlreadyClosedAndReturnNull();
        }

        try {
            return handleSnapshotTxDataRequestInternal(request);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private SnapshotTxDataResponse handleSnapshotTxDataRequestInternal(SnapshotTxDataRequest request) {
        List<IgniteBiTuple<UUID, TxMeta>> rows = new ArrayList<>();

        boolean finishedTxData = this.finishedTxData;

        Cursor<IgniteBiTuple<UUID, TxMeta>> txDataCursor = this.txDataCursor;

        assert txDataCursor != null : "Snapshot scope has not been frozen.";

        while (!finishedTxData && rows.size() < request.maxTransactionsInBatch()) {
            if (txDataCursor.hasNext()) {
                rows.add(txDataCursor.next());
            } else {
                finishedTxData = true;
                txDataCursor.close();
            }
        }

        this.finishedTxData = finishedTxData;

        return buildTxDataResponse(rows, finishedTxData);
    }

    private static SnapshotTxDataResponse buildTxDataResponse(List<IgniteBiTuple<UUID, TxMeta>> rows, boolean finished) {
        var txIds = new ArrayList<UUID>(rows.size());
        var txMetas = new ArrayList<TxMetaMessage>(rows.size());

        for (IgniteBiTuple<UUID, TxMeta> row : rows) {
            txIds.add(row.getKey());
            txMetas.add(row.getValue().toTransactionMetaMessage(REPLICA_MESSAGES_FACTORY, TX_MESSAGES_FACTORY));
        }

        return PARTITION_REPLICATION_MESSAGES_FACTORY.snapshotTxDataResponse()
                .txIds(txIds)
                .txMeta(txMetas)
                .finish(finished)
                .build();
    }

    /**
     * Acquires lock over this snapshot MV data.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
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
     * Returns {@code true} if this snapshot is finished with sending MV data (i.e. it already sent all MV data and is not going to send
     * anything else).
     *
     * <p>Must be called under {@link #mvOperationsLock}.
     */
    private boolean finishedMvData() {
        assert mvOperationsLock.isLocked() : "MV operations lock must be acquired!";

        return mvPartitionDeliveryState != null && mvPartitionDeliveryState.isExhausted();
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
     * Returns {@code true} if the given {@link RowId} does not interfere with the rows that this snapshot is going to send in the normal
     * snapshot rows sending order.
     *
     * <p>Must be called under MV data snapshot lock.
     *
     * @param rowId RowId.
     * @return {@code true} if the given RowId is already passed by the snapshot in normal rows sending order or if the RowId belongs to
     *     a table that is not a part of this snapshot at all.
     */
    public boolean alreadyPassedOrIrrelevant(int tableId, RowId rowId) {
        assert mvOperationsLock.isLocked() : "MV operations lock must be acquired!";

        if (mvPartitionDeliveryState == null) {
            // We haven't even frozen the snapshot scope yet.
            return false;
        }

        return !mvPartitionDeliveryState.isGoingToBeDelivered(tableId) || alreadyPassed(tableId, rowId);
    }

    private boolean alreadyPassed(int tableId, RowId rowId) {
        assert mvPartitionDeliveryState != null;

        if (mvPartitionDeliveryState.isExhausted()) {
            // We have already finished delivering all data.
            return true;
        }

        if (!mvPartitionDeliveryState.hasIterationStarted()) {
            // We haven't started streaming the data yet.
            return false;
        }

        if (tableId == mvPartitionDeliveryState.currentTableId()) {
            // 'currentRowId' here has already been sent, hence the non-strict comparison.
            return rowId.compareTo(mvPartitionDeliveryState.currentRowId()) <= 0;
        } else {
            return tableId < mvPartitionDeliveryState.currentTableId();
        }
    }

    /**
     * Enqueues a row for out-of-order sending.
     *
     * <p>Must be called under snapshot lock.
     *
     * @param rowId {@link RowId} of the row.
     */
    public void enqueueForSending(int tableId, RowId rowId) {
        assert mvOperationsLock.isLocked() : "MV operations lock must be acquired!";

        ResponseEntry entry = rowEntry(partitionsByTableId.get(tableId), rowId);

        if (entry != null) {
            outOfOrderMvData.add(entry);
        }
    }

    /**
     * Closes the snapshot releasing the underlying resources.
     */
    public void close() {
        if (!closedGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        snapshotStats.onSnapshotEnd();
        snapshotsMetricsSource.onOutgoingSnapshotEnd();

        snapshotStats.logSnapshotStats();

        if (!finishedTxData) {
            Cursor<IgniteBiTuple<UUID, TxMeta>> txCursor = txDataCursor;

            if (txCursor != null) {
                txCursor.close();
                finishedTxData = true;
            }
        }
    }
}
