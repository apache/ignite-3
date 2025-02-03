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
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowUpgrader;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/** {@link PartitionAccess} implementation. */
public class PartitionAccessImpl implements PartitionAccess {
    private final PartitionKey partitionKey;

    private final MvTableStorage mvTableStorage;

    private final TxStateStorage txStateStorage;

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
     * @param partitionKey Partition key.
     * @param mvTableStorage Multi version table storage.
     * @param txStateStorage Table transaction state storage.
     * @param mvGc Garbage collector for multi-versioned storages and their indexes in the background.
     * @param indexUpdateHandler Index update handler.
     * @param gcUpdateHandler Gc update handler.
     * @param fullStateTransferIndexChooser Index chooser for full state transfer.
     * @param schemaRegistry Schema registry.
     * @param lowWatermark Low watermark.
     */
    public PartitionAccessImpl(
            PartitionKey partitionKey,
            MvTableStorage mvTableStorage,
            TxStateStorage txStateStorage,
            MvGc mvGc,
            IndexUpdateHandler indexUpdateHandler,
            GcUpdateHandler gcUpdateHandler,
            FullStateTransferIndexChooser fullStateTransferIndexChooser,
            SchemaRegistry schemaRegistry,
            LowWatermark lowWatermark
    ) {
        this.partitionKey = partitionKey;
        this.mvTableStorage = mvTableStorage;
        this.txStateStorage = txStateStorage;
        this.mvGc = mvGc;
        this.indexUpdateHandler = indexUpdateHandler;
        this.gcUpdateHandler = gcUpdateHandler;
        this.fullStateTransferIndexChooser = fullStateTransferIndexChooser;
        this.schemaRegistry = schemaRegistry;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public PartitionKey partitionKey() {
        return partitionKey;
    }

    private int partitionId() {
        return partitionKey.partitionId();
    }

    private int tableId() {
        return mvTableStorage.getTableDescriptor().getId();
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> getAllTxMeta() {
        return getTxStateStorage().scan();
    }

    @Override
    public void addTxMeta(UUID txId, TxMeta txMeta) {
        getTxStateStorage().putForRebalance(txId, txMeta);
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
    public void addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, int commitTableId, int commitPartitionId, int catalogVersion) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage();

        List<IndexIdAndTableVersion> indexIdAndTableVersionList = fullStateTransferIndexChooser.chooseForAddWrite(
                catalogVersion,
                tableId(),
                beginTimestamp(txId)
        );

        List<IndexIdAndBinaryRow> indexIdAndBinaryRowList = upgradeForEachTableVersion(row, indexIdAndTableVersionList);

        mvPartitionStorage.runConsistently(locker -> {
            locker.lock(rowId);

            mvPartitionStorage.addWrite(rowId, row, txId, commitTableId, commitPartitionId);

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

            mvPartitionStorage.addWriteCommitted(rowId, row, commitTimestamp);

            for (IndexIdAndBinaryRow indexIdAndBinaryRow : indexIdAndBinaryRowList) {
                indexUpdateHandler.addToIndex(indexIdAndBinaryRow.binaryRow(), rowId, indexIdAndBinaryRow.indexId());
            }

            return null;
        });
    }

    @Override
    public long minLastAppliedIndex() {
        return Math.min(
                getMvPartitionStorage().lastAppliedIndex(),
                getTxStateStorage().lastAppliedIndex()
        );
    }

    @Override
    public long minLastAppliedTerm() {
        return Math.min(
                getMvPartitionStorage().lastAppliedTerm(),
                getTxStateStorage().lastAppliedTerm()
        );
    }

    @Override
    public long maxLastAppliedIndex() {
        return Math.max(
                getMvPartitionStorage().lastAppliedIndex(),
                getTxStateStorage().lastAppliedIndex()
        );
    }

    @Override
    public long maxLastAppliedTerm() {
        return Math.max(
                getMvPartitionStorage().lastAppliedTerm(),
                getTxStateStorage().lastAppliedTerm()
        );
    }

    @Override
    public long leaseStartTime() {
        return getMvPartitionStorage().leaseStartTime();
    }

    @Override
    public @Nullable UUID primaryReplicaNodeId() {
        return getMvPartitionStorage().primaryReplicaNodeId();
    }

    @Override
    public @Nullable String primaryReplicaNodeName() {
        return getMvPartitionStorage().primaryReplicaNodeName();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        TxStatePartitionStorage txStatePartitionStorage = getTxStateStorage();

        return mvGc.removeStorage(toTablePartitionId(partitionKey))
                .thenCompose(unused -> CompletableFuture.allOf(
                        mvTableStorage.startRebalancePartition(partitionId()),
                        txStatePartitionStorage.startRebalance()
                ));
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        TxStatePartitionStorage txStatePartitionStorage = getTxStateStorage();

        return CompletableFuture.allOf(
                mvTableStorage.abortRebalancePartition(partitionId()),
                txStatePartitionStorage.abortRebalance()
        ).thenAccept(unused -> mvGc.addStorage(toTablePartitionId(partitionKey), gcUpdateHandler));
    }

    @Override
    public CompletableFuture<Void> finishRebalance(RaftSnapshotPartitionMeta partitionMeta) {
        TxStatePartitionStorage txStatePartitionStorage = getTxStateStorage();

        byte[] configBytes = raftGroupConfigurationConverter.toBytes(partitionMeta.raftGroupConfig());

        return CompletableFuture.allOf(
                mvTableStorage.finishRebalancePartition(partitionId(), partitionMeta.toMvPartitionMeta(configBytes)),
                txStatePartitionStorage.finishRebalance(partitionMeta.lastAppliedIndex(), partitionMeta.lastAppliedTerm())
        ).thenAccept(unused -> mvGc.addStorage(toTablePartitionId(partitionKey), gcUpdateHandler));
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

    private MvPartitionStorage getMvPartitionStorage() {
        int partitionId = partitionId();

        MvPartitionStorage mvPartitionStorage = mvTableStorage.getMvPartition(partitionId);

        assert mvPartitionStorage != null : IgniteStringFormatter.format("tableId={}, partitionId={}", tableId(), partitionId);

        return mvPartitionStorage;
    }

    private TxStatePartitionStorage getTxStateStorage() {
        int partitionId = partitionId();

        TxStatePartitionStorage txStatePartitionStorage = txStateStorage.getPartitionStorage(partitionId);

        assert txStatePartitionStorage != null : IgniteStringFormatter.format("tableId={}, partitionId={}", tableId(), partitionId);

        return txStatePartitionStorage;
    }

    private static TablePartitionId toTablePartitionId(PartitionKey partitionKey) {
        return new TablePartitionId(partitionKey.tableId(), partitionKey.partitionId());
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
