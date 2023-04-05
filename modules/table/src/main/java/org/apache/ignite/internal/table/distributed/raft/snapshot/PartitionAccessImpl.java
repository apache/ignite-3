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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * {@link PartitionAccess} implementation.
 */
public class PartitionAccessImpl implements PartitionAccess {
    private final PartitionKey partitionKey;

    private final MvTableStorage mvTableStorage;

    private final TxStateTableStorage txStateTableStorage;

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    private final StorageUpdateHandler storageUpdateHandler;

    private final MvGc mvGc;

    /**
     * Constructor.
     *
     * @param partitionKey Partition key.
     * @param mvTableStorage Multi version table storage.
     * @param txStateTableStorage Table transaction state storage.
     * @param storageUpdateHandler Storage update handler.
     * @param mvGc Garbage collector for multi-versioned storages and their indexes in the background.
     */
    public PartitionAccessImpl(
            PartitionKey partitionKey,
            MvTableStorage mvTableStorage,
            TxStateTableStorage txStateTableStorage,
            StorageUpdateHandler storageUpdateHandler,
            MvGc mvGc
    ) {
        this.partitionKey = partitionKey;
        this.mvTableStorage = mvTableStorage;
        this.txStateTableStorage = txStateTableStorage;
        this.storageUpdateHandler = storageUpdateHandler;
        this.mvGc = mvGc;
    }

    @Override
    public PartitionKey partitionKey() {
        return partitionKey;
    }

    private int partitionId() {
        return partitionKey.partitionId();
    }

    private String tableName() {
        return mvTableStorage.configuration().name().value();
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> getAllTxMeta() {
        return getTxStateStorage(partitionId()).scan();
    }

    @Override
    public void addTxMeta(UUID txId, TxMeta txMeta) {
        getTxStateStorage(partitionId()).put(txId, txMeta);
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) {
        return getMvPartitionStorage(partitionId()).closestRowId(lowerBound);
    }

    @Override
    public Cursor<ReadResult> getAllRowVersions(RowId rowId) {
        return getMvPartitionStorage(partitionId()).scanVersions(rowId);
    }

    @Override
    public @Nullable RaftGroupConfiguration committedGroupConfiguration() {
        byte[] configBytes = getMvPartitionStorage(partitionId()).committedGroupConfiguration();

        return raftGroupConfigurationConverter.fromBytes(configBytes);
    }

    @Override
    public void addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage(partitionId());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWrite(rowId, row, txId, commitTableId, commitPartitionId);

            storageUpdateHandler.addToIndexes(row, rowId);

            return null;
        });
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage(partitionId());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(rowId, row, commitTimestamp);

            storageUpdateHandler.addToIndexes(row, rowId);

            return null;
        });
    }

    @Override
    public long minLastAppliedIndex() {
        return Math.min(
                getMvPartitionStorage(partitionId()).lastAppliedIndex(),
                getTxStateStorage(partitionId()).lastAppliedIndex()
        );
    }

    @Override
    public long minLastAppliedTerm() {
        return Math.min(
                getMvPartitionStorage(partitionId()).lastAppliedTerm(),
                getTxStateStorage(partitionId()).lastAppliedTerm()
        );
    }

    @Override
    public long maxLastAppliedIndex() {
        return Math.max(
                getMvPartitionStorage(partitionId()).lastAppliedIndex(),
                getTxStateStorage(partitionId()).lastAppliedIndex()
        );
    }

    @Override
    public long maxLastAppliedTerm() {
        return Math.max(
                getMvPartitionStorage(partitionId()).lastAppliedTerm(),
                getTxStateStorage(partitionId()).lastAppliedTerm()
        );
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        // TODO: IGNITE-18619 Fix it, we should have already waited for the indexes to be created
        storageUpdateHandler.waitIndexes();

        TxStateStorage txStateStorage = getTxStateStorage(partitionId());

        return mvGc.removeStorage(toTablePartitionId(partitionKey))
                .thenCompose(unused -> CompletableFuture.allOf(
                        mvTableStorage.startRebalancePartition(partitionId()),
                        txStateStorage.startRebalance()
                ));
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        TxStateStorage txStateStorage = getTxStateStorage(partitionId());

        return CompletableFuture.allOf(
                mvTableStorage.abortRebalancePartition(partitionId()),
                txStateStorage.abortRebalance()
        ).thenAccept(unused -> mvGc.addStorage(toTablePartitionId(partitionKey), storageUpdateHandler));
    }

    @Override
    public CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration raftGroupConfig) {
        TxStateStorage txStateStorage = getTxStateStorage(partitionId());

        byte[] configBytes = raftGroupConfigurationConverter.toBytes(raftGroupConfig);

        return CompletableFuture.allOf(
                mvTableStorage.finishRebalancePartition(partitionId(), lastAppliedIndex, lastAppliedTerm, configBytes),
                txStateStorage.finishRebalance(lastAppliedIndex, lastAppliedTerm)
        ).thenAccept(unused -> mvGc.addStorage(toTablePartitionId(partitionKey), storageUpdateHandler));
    }

    private MvPartitionStorage getMvPartitionStorage(int partitionId) {
        MvPartitionStorage mvPartitionStorage = mvTableStorage.getMvPartition(partitionId);

        assert mvPartitionStorage != null : IgniteStringFormatter.format("table={}, partitionId={}", tableName(), partitionId);

        return mvPartitionStorage;
    }

    private TxStateStorage getTxStateStorage(int partitionId) {
        TxStateStorage txStateStorage = txStateTableStorage.getTxStateStorage(partitionId);

        assert txStateStorage != null : IgniteStringFormatter.format("table={}, partitionId={}", tableName(), partitionId);

        return txStateStorage;
    }

    private static TablePartitionId toTablePartitionId(PartitionKey partitionKey) {
        return new TablePartitionId(partitionKey.tableId(), partitionKey.partitionId());
    }
}
