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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowUpgrader;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.AddWriteCommittedResult;
import org.apache.ignite.internal.storage.AddWriteCommittedResultStatus;
import org.apache.ignite.internal.storage.AddWriteResult;
import org.apache.ignite.internal.storage.AddWriteResultStatus;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/** {@link PartitionMvStorageAccess} implementation. */
public class PartitionMvStorageAccessImpl implements PartitionMvStorageAccess {
    private final int partitionId;

    private final MvTableStorage mvTableStorage;

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    private final MvGc mvGc;

    private final IndexUpdateHandler indexUpdateHandler;

    private final GcUpdateHandler gcUpdateHandler;

    private final FullStateTransferIndexChooser fullStateTransferIndexChooser;

    private final SchemaRegistry schemaRegistry;

    private final LowWatermark lowWatermark;

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
     * @param mvTableStorage Multi version table storage.
     * @param mvGc Garbage collector for multi-versioned storages and their indexes in the background.
     * @param indexUpdateHandler Index update handler.
     * @param gcUpdateHandler Gc update handler.
     * @param fullStateTransferIndexChooser Index chooser for full state transfer.
     * @param schemaRegistry Schema registry.
     * @param lowWatermark Low watermark.
     */
    public PartitionMvStorageAccessImpl(
            int partitionId,
            MvTableStorage mvTableStorage,
            MvGc mvGc,
            IndexUpdateHandler indexUpdateHandler,
            GcUpdateHandler gcUpdateHandler,
            FullStateTransferIndexChooser fullStateTransferIndexChooser,
            SchemaRegistry schemaRegistry,
            LowWatermark lowWatermark
    ) {
        this.partitionId = partitionId;
        this.mvTableStorage = mvTableStorage;
        this.mvGc = mvGc;
        this.indexUpdateHandler = indexUpdateHandler;
        this.gcUpdateHandler = gcUpdateHandler;
        this.fullStateTransferIndexChooser = fullStateTransferIndexChooser;
        this.schemaRegistry = schemaRegistry;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public int partitionId() {
        return partitionId;
    }

    @Override
    public int tableId() {
        return mvTableStorage.getTableDescriptor().getId();
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) {
        return getMvPartitionStorage().closestRowId(lowerBound);
    }

    @Override
    public List<ReadResult> getAllRowVersions(RowId rowId) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage();

        return mvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            try (Cursor<ReadResult> cursor = mvPartitionStorage.scanVersions(rowId)) {
                return cursor.stream().collect(toList());
            }
        });
    }

    @Override
    public @Nullable RaftGroupConfiguration committedGroupConfiguration() {
        byte[] configBytes = getMvPartitionStorage().committedGroupConfiguration();

        return raftGroupConfigurationConverter.fromBytes(configBytes);
    }

    @Override
    public void addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId,
            int catalogVersion
    ) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage();

        List<IndexIdAndTableVersion> indexIdAndTableVersionList = fullStateTransferIndexChooser.chooseForAddWrite(
                catalogVersion,
                tableId(),
                beginTimestamp(txId)
        );

        List<IndexIdAndBinaryRow> indexIdAndBinaryRowList = upgradeForEachTableVersion(row, indexIdAndTableVersionList);

        mvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            AddWriteResult result = mvPartitionStorage.addWrite(rowId, row, txId, commitZoneId, commitPartitionId);

            if (result.status() == AddWriteResultStatus.TX_MISMATCH) {
                throw new TxIdMismatchException(result.currentWriteIntentTxId(), txId);
            }

            for (IndexIdAndBinaryRow indexIdAndBinaryRow : indexIdAndBinaryRowList) {
                indexUpdateHandler.addToIndex(indexIdAndBinaryRow.binaryRow(), rowId, indexIdAndBinaryRow.indexId());
            }

            return null;
        });
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp, int catalogVersion) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage();

        List<IndexIdAndTableVersion> indexIdAndTableVersionList = fullStateTransferIndexChooser.chooseForAddWriteCommitted(
                catalogVersion,
                tableId(),
                commitTimestamp
        );

        List<IndexIdAndBinaryRow> indexIdAndBinaryRowList = upgradeForEachTableVersion(row, indexIdAndTableVersionList);

        mvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            AddWriteCommittedResult result = mvPartitionStorage.addWriteCommitted(rowId, row, commitTimestamp);

            if (result.status() == AddWriteCommittedResultStatus.WRITE_INTENT_EXISTS) {
                throw new StorageException("Write intent already exists: [rowId={}]", rowId);
            }

            for (IndexIdAndBinaryRow indexIdAndBinaryRow : indexIdAndBinaryRowList) {
                indexUpdateHandler.addToIndex(indexIdAndBinaryRow.binaryRow(), rowId, indexIdAndBinaryRow.indexId());
            }

            return null;
        });
    }

    @Override
    public long lastAppliedIndex() {
        return getMvPartitionStorage().lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return getMvPartitionStorage().lastAppliedTerm();
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return getMvPartitionStorage().leaseInfo();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        return mvGc.removeStorage(tablePartitionId())
                .thenCompose(unused -> mvTableStorage.startRebalancePartition(partitionId()));
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        return mvTableStorage.abortRebalancePartition(partitionId())
                .thenAccept(unused -> mvGc.addStorage(tablePartitionId(), gcUpdateHandler));
    }

    @Override
    public CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta) {
        return mvTableStorage.finishRebalancePartition(partitionId(), partitionMeta)
                .thenAccept(v -> mvGc.addStorage(tablePartitionId(), gcUpdateHandler))
                .thenCompose(v -> getMvPartitionStorage().flush());
    }

    @Override
    public @Nullable RowId getNextRowIdToBuildIndex(int indexId) {
        return indexUpdateHandler.getNextRowIdToBuildIndex(indexId);
    }

    @Override
    public void setNextRowIdToBuildIndex(Map<Integer, RowId> nextRowIdToBuildByIndexId) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage();

        mvPartitionStorage.runConsistently(locker -> {
            nextRowIdToBuildByIndexId.forEach(indexUpdateHandler::setNextRowIdToBuildIndex);

            return null;
        });
    }

    @Override
    public void updateLowWatermark(HybridTimestamp newLowWatermark) {
        lowWatermark.updateLowWatermark(newLowWatermark);
    }

    @Override
    public boolean isVolatile() {
        return mvTableStorage.isVolatile();
    }

    private MvPartitionStorage getMvPartitionStorage() {
        int partitionId = partitionId();

        MvPartitionStorage mvPartitionStorage = mvTableStorage.getMvPartition(partitionId);

        assert mvPartitionStorage != null : IgniteStringFormatter.format("tableId={}, partitionId={}", tableId(), partitionId);

        return mvPartitionStorage;
    }

    private TablePartitionId tablePartitionId() {
        return new TablePartitionId(tableId(), partitionId);
    }

    private List<IndexIdAndBinaryRow> upgradeForEachTableVersion(
            @Nullable BinaryRow source,
            List<IndexIdAndTableVersion> indexIdAndTableVersionList
    ) {
        if (source == null) { // Skip removes.
            return List.of();
        }

        return indexIdAndTableVersionList.stream()
                .map(indexIdAndTableVersion -> {
                    var upgrader = new BinaryRowUpgrader(schemaRegistry, indexIdAndTableVersion.tableVersion());

                    return new IndexIdAndBinaryRow(indexIdAndTableVersion.indexId(), upgrader.upgrade(source));
                })
                .collect(toCollection(() -> new ArrayList<>(indexIdAndTableVersionList.size())));
    }
}
