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

import static org.apache.ignite.internal.table.distributed.TableManager.FULL_RABALANCING_STARTED;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
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

    /**
     * Constructor.
     *
     * @param partitionKey Partition key.
     * @param mvTableStorage Multi version table storage.
     * @param txStateTableStorage Table transaction state storage.
     */
    public PartitionAccessImpl(
            PartitionKey partitionKey,
            MvTableStorage mvTableStorage,
            TxStateTableStorage txStateTableStorage
    ) {
        this.partitionKey = partitionKey;
        this.mvTableStorage = mvTableStorage;
        this.txStateTableStorage = txStateTableStorage;
    }

    @Override
    public PartitionKey partitionKey() {
        return partitionKey;
    }

    @Override
    public MvPartitionStorage mvPartitionStorage() {
        MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(partitionId());

        assert mvPartition != null : "table=" + tableName() + ", part=" + partitionId();

        return mvPartition;
    }

    @Override
    public TxStateStorage txStatePartitionStorage() {
        TxStateStorage txStatePartitionStorage = txStateTableStorage.getTxStateStorage(partitionId());

        assert txStatePartitionStorage != null : "table=" + tableName() + ", part=" + partitionId();

        return txStatePartitionStorage;
    }

    @Override
    public CompletableFuture<Void> reCreateMvPartitionStorage() throws StorageException {
        assert mvTableStorage.getMvPartition(partitionId()) != null : "table=" + tableName() + ", part=" + partitionId();

        // TODO: IGNITE-18030 - actually recreate or do in a different way
        //return mvTableStorage.destroyPartition(partId())
        return CompletableFuture.completedFuture(null)
                .thenApply(unused -> {
                    MvPartitionStorage mvPartitionStorage = mvTableStorage.getOrCreateMvPartition(partitionId());

                    mvPartitionStorage.runConsistently(() -> {
                        mvPartitionStorage.lastApplied(FULL_RABALANCING_STARTED, 0);

                        return null;
                    });

                    return null;
                });
    }

    @Override
    public void reCreateTxStatePartitionStorage() throws StorageException {
        assert txStateTableStorage.getTxStateStorage(partitionId()) != null : "table=" + tableName() + ", part=" + partitionId();

        // TODO: IGNITE-18030 - actually recreate or do in a different way
        //txStateTableStorage.destroyTxStateStorage(partId());

        txStateTableStorage.getOrCreateTxStateStorage(partitionId()).lastApplied(FULL_RABALANCING_STARTED, 0);
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
        return getMvPartitionStorage(partitionId()).committedGroupConfiguration();
    }

    @Override
    public void addWrite(RowId rowId, BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage(partitionId());

        mvPartitionStorage.runConsistently(() -> mvPartitionStorage.addWrite(rowId, row, txId, commitTableId, commitPartitionId));
    }

    @Override
    public void addWriteCommitted(RowId rowId, BinaryRow row, HybridTimestamp commitTimestamp) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage(partitionId());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.addWriteCommitted(rowId, row, commitTimestamp);

            return null;
        });
    }

    @Override
    public void updateLastApplied(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration raftGroupConfig) {
        MvPartitionStorage mvPartitionStorage = getMvPartitionStorage(partitionId());
        TxStateStorage txStateStorage = getTxStateStorage(partitionId());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);

            txStateStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);

            mvPartitionStorage.committedGroupConfiguration(raftGroupConfig);

            return null;
        });
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
}
